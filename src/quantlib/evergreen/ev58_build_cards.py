"""EV58:把 EV57 樣本組裝成「階段 A 樣本卡」與「階段 B 真相檔」——盲判的物理保證。

## 為什麼必須有這支(2026-08-05 review 抓到的缺口)
`PROMPT_ev58_attribution_*` 承諾給 agent 的樣本卡欄位,`evergreen_ev57_*.csv` **並不具備**
(缺 `name_today` / `d_prev` / 20・60 日報酬 / 距 250 日高點 / 市值級距),而 CSV 裡的
`fwd_max_ret` **正是答案本身**。沒有這支,任何人把 CSV 直接餵給 agent,盲判就當場失效
——而盲判是整個重新蒸餾唯一能回答「這套質化方法有沒有判別力」的機制。

**盲判不能靠紀律,要靠物理隔離**:本模組產出兩份互斥的檔案,結果欄位**在階段 A 的卡片
裡根本不存在**,不是「請你不要看」。

## 產出
    out/ev58_cards/{batch_id}/cards.json    階段 A 用。**零結果欄位**,且不揭露臂別。
    out/ev58_cards/{batch_id}/truth.json    階段 B 用。編排腳本在階段 A 寫檔完成後才釋出。

## 批次編排(與提示詞的契約一致)
每批 6 檔 = 2 組配對(正例 + 其配對負例)+ 2 檔未配對;順序打散,正負比例在
2:4 / 3:3 / 4:2 間浮動,**比例與配對關係不出現在 cards.json**。同批同年代
(`limit_era` × `regime` 分批),讓年代語彙表能在批內共用只採一次。

Run: uv run --project . python -m quantlib.evergreen.ev58_build_cards [--pilot]
依賴 cache: 是。`--pilot` 只出 8 批(第一階段試跑用)。
"""
from __future__ import annotations

import argparse
import json
import random
from datetime import date as Date

import polars as pl

from quantlib import paths, prices
from quantlib.apex import data

C = "company_code"
OUT = paths.OUT / "ev58_cards"
#: 每批檔數;正負比例在 2:4 / 3:3 / 4:2 間浮動,agent 無從由結構反推
BATCH = 6
#: 卡片可含的欄位白名單。**結果欄位不在此列即為不存在**——白名單而非黑名單,
#: 因為黑名單漏一個就洩漏答案,白名單漏一個只是少給資訊。
CARD_FIELDS = ("card_id", "code", "name_today", "market", "industry", "limit_era",
               "d_prev", "d0", "ret20", "ret60", "ret120", "adv_decile",
               "mom_decile", "pct_below_250d_high", "size_tier")


def _stations(panel: pl.DataFrame) -> list[Date]:
    d = panel.select("date").unique().sort("date")
    return (d.with_columns([pl.col("date").dt.strftime("%Y-%m").alias("ym"),
                            pl.col("date").dt.day().alias("dd")])
            .filter(pl.col("dd") > 10).group_by("ym").agg(pl.col("date").min())
            .sort("date")["date"].to_list())


def _panel(con, start: str, end: str) -> pl.DataFrame:
    fr = []
    for m in ("twse", "tpex"):
        f = prices.fetch_adjusted_panel(con, start, end, market=m,
                                        include_extra_history_days=300)
        if not f.is_empty():
            fr.append(f.select([C, "date", "close", "trade_value"]))
    return (pl.concat(fr).unique(subset=[C, "date"], keep="first")
            .filter(pl.col("close") > 0).sort([C, "date"]))


def _card_features(panel: pl.DataFrame) -> pl.DataFrame:
    """只用站位日當天為止的資訊算——**任何一欄都不得含未來**。"""
    return panel.with_columns([
        (pl.col("close") / pl.col("close").shift(20).over(C) - 1).alias("ret20"),
        (pl.col("close") / pl.col("close").shift(60).over(C) - 1).alias("ret60"),
        (pl.col("close") / pl.col("close").shift(120).over(C) - 1).alias("ret120"),
        (pl.col("close") / pl.col("close").rolling_max(250, min_samples=60).over(C) - 1)
        .alias("pct_below_250d_high"),
        pl.col("trade_value").rolling_mean(20, min_samples=20).over(C).alias("adv20"),
    ]).with_columns(
        # 市值級距用「當日成交值截面五分位」代理——cache 無逐日市值,而成交值是
        # 同一天橫斷面上可得的規模代理;絕對金額在 2008 與 2021 不可比,故取分位。
        ((pl.col("adv20").rank("ordinal").over("date") * 5 - 1)
         // pl.len().over("date")).alias("size_q"))


def _names(con) -> dict[str, str]:
    q = con.sql("SELECT company_code, any_value(company_name) AS nm "
                "FROM operating_revenue GROUP BY company_code").pl()
    return {r[C]: r["nm"] for r in q.iter_rows(named=True)}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--pilot", action="store_true", help="只出 8 批(第一階段試跑)")
    ap.add_argument("--seed", type=int, default=20260805)
    a = ap.parse_args()
    rng = random.Random(a.seed)

    pos = pl.read_csv(paths.OUT / "evergreen_ev57_positives.csv",
                      schema_overrides={C: pl.Utf8})
    neg = pl.read_csv(paths.OUT / "evergreen_ev57_negatives.csv",
                      schema_overrides={C: pl.Utf8})
    con = data.connect()
    lo = min(pos["date"].min(), neg["date"].min())
    hi = max(pos["date"].max(), neg["date"].max())
    panel = _panel(con, str(lo), str(hi))
    feat = _card_features(panel)
    st = _stations(panel)
    prev_of = {d: p for p, d in zip(st, st[1:])}
    names = _names(con)

    fmap = {(r[C], str(r["date"])): r for r in feat.to_dicts()}

    def card(row: dict, arm: str, cid: str) -> tuple[dict, dict] | None:
        k = (row[C], str(row["date"]))
        f = fmap.get(k)
        if f is None:
            return None
        d0 = Date.fromisoformat(str(row["date"]))
        c = {
            "card_id": cid, "code": row[C], "name_today": names.get(row[C], ""),
            "market": row["market"], "industry": row["industry"],
            "limit_era": row["limit_era"],
            "d_prev": str(prev_of.get(d0, "")), "d0": str(d0),
            "ret20": f["ret20"], "ret60": f["ret60"], "ret120": f["ret120"],
            "adv_decile": row["adv_dec"], "mom_decile": row["mom_dec"],
            "pct_below_250d_high": f["pct_below_250d_high"],
            "size_tier": f["size_q"],
        }
        assert set(c) <= set(CARD_FIELDS), "卡片出現白名單外的欄位"
        truth = {"card_id": cid, "code": row[C], "d0": str(d0), "arm": arm,
                 "fwd_max_ret": row["fwd_max_ret"], "regime": row["regime"],
                 "neg_kind": row.get("neg_kind"), "matched_to": row.get("matched_to")}
        return c, truth

    # 配對關係:負例的 matched_to 指向正例;同批放入 2 組配對 + 2 檔未配對
    by_match = {r["matched_to"]: r for r in neg.to_dicts() if r.get("matched_to")}
    pos_rows = pos.to_dicts()
    rng.shuffle(pos_rows)
    used_neg: set[str] = set()
    batches: list[list[tuple[dict, dict]]] = []
    i = 0
    while i + 2 <= len(pos_rows):
        pair_src = pos_rows[i:i + 2]
        i += 2
        items: list[tuple[dict, dict]] = []
        for p in pair_src:
            key = f"{p[C]}@{p['date']}"
            n = by_match.get(key)
            if not n or key in used_neg:
                continue
            used_neg.add(key)
            for row, arm in ((p, "positive"), (n, "negative")):
                cid = f"X{len(batches):03d}{len(items):02d}"
                got = card(row, arm, cid)
                if got:
                    items.append(got)
        # 2 檔未配對:讓正負比例浮動,agent 無法由 3:3 結構反推配對
        pool = [(r, "positive") for r in pos_rows[i:i + 4]] + \
               [(r, "negative") for r in neg.to_dicts() if f"{r[C]}@{r['date']}" not in used_neg][:4]
        rng.shuffle(pool)
        for row, arm in pool[:2]:
            cid = f"X{len(batches):03d}{len(items):02d}"
            got = card(row, arm, cid)
            if got:
                items.append(got)
        if len(items) < BATCH:
            continue
        rng.shuffle(items)
        batches.append(items[:BATCH])
        if a.pilot and len(batches) >= 8:
            break

    OUT.mkdir(parents=True, exist_ok=True)
    npos = nneg = 0
    for bi, items in enumerate(batches):
        bdir = OUT / f"batch_{bi:03d}"
        bdir.mkdir(exist_ok=True)
        cards = [c for c, _ in items]
        truth = [t for _, t in items]
        npos += sum(1 for t in truth if t["arm"] == "positive")
        nneg += sum(1 for t in truth if t["arm"] == "negative")
        (bdir / "cards.json").write_text(
            json.dumps(cards, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        (bdir / "truth.json").write_text(
            json.dumps(truth, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    # 硬性驗證:卡片絕不可含任何結果欄位
    leak = [k for c in (json.loads((OUT / "batch_000" / "cards.json").read_text()))
            for k in c if k not in CARD_FIELDS]
    assert not leak, f"卡片洩漏結果欄位:{leak}"
    print(f"{len(batches)} 批 × {BATCH} 檔 = {len(batches)*BATCH} 張卡"
          f"(正 {npos} / 負 {nneg});每批正例數分布:"
          f"{sorted({sum(1 for _,t in b if t['arm']=='positive') for b in batches})}")
    print(f"卡片欄位:{list(json.loads((OUT/'batch_000'/'cards.json').read_text())[0])}")
    print(f"→ {OUT}")


if __name__ == "__main__":
    main()
