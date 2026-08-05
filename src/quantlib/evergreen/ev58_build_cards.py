"""EV58:把 EV57 樣本組裝成「階段 A 樣本卡」與「階段 B 真相檔」——盲判的物理保證。

## 為什麼必須有這支(2026-08-05 review 抓到的缺口)
`PROMPT_ev58_attribution_*` 承諾給 agent 的樣本卡欄位,`evergreen_ev57_*.csv` **並不具備**
(缺 `name_today` / `d_prev` / 20・60 日報酬 / 距 250 日高點 / 市值級距),而 CSV 裡的
`fwd_max_ret` **正是答案本身**。沒有這支,任何人把 CSV 直接餵給 agent,盲判就當場失效
——而盲判是整個重新蒸餾唯一能回答「這套質化方法有沒有判別力」的機制。

**盲判不能靠紀律,要靠物理隔離**:本模組產出兩份互斥的檔案,結果欄位**在階段 A 的卡片
裡根本不存在**,不是「請你不要看」。

## 產出(**兩個互斥的根**——這是第六輪 review 修掉的缺口)
    out/ev58_cards/{batch_id}/cards.json    階段 A 用。**零結果欄位**,且不揭露臂別。
    out/ev58_truth/{batch_id}.json          真相。**從不交給任何 agent**。

真相原本與卡片同目錄,而歸因 agent 從第一秒就拿著那個目錄的路徑——「物理隔離」
於是退化成「請你不要看」,正是本設計宣稱要取代的東西。現在真相在另一個根,
由 `ev58_prompt.release()` 驗明該批階段 A 全數落檔後,才逐檔複製進 agent 看得到的
`ev58_news/_truth/`;缺任一檔即整批拒絕釋出。

## 批次編排(與提示詞的契約一致)
每批 6 檔 = 2 組配對(正例 + 其配對負例)+ 2 檔未配對;順序打散,正負比例在
2:4 / 3:3 / 4:2 間浮動,**比例與配對關係不出現在 cards.json**。同批同年代
(`limit_era` × `regime` 分批),讓年代語彙表能在批內共用只採一次。

Run: uv run --project . python -m quantlib.evergreen.ev58_build_cards [--pilot]
依賴 cache: 是。`--pilot` **每個年代桶各出 1 批**(6 批 × 6 檔 = 36 張),
第一階段試跑用——試跑的唯一目的是量各年代的消息可得性,全押同一年代等於白跑。
"""
from __future__ import annotations

import argparse
import itertools
import json
import random
from datetime import date as Date

import polars as pl

from quantlib import paths, prices
from quantlib.apex import data

C = "company_code"
OUT = paths.OUT / "ev58_cards"
#: 真相的權威位置。與 `OUT` 分根是**承重設計**,不是整理癖:同根就等於把答案
#: 放進 agent 已被授予的目錄裡,盲判只剩自律。
TRUTH = paths.OUT / "ev58_truth"
#: 每批檔數;正負比例在 2:4 / 3:3 / 4:2 間浮動,agent 無從由結構反推
BATCH = 6
#: 卡片可含的欄位白名單。**結果欄位不在此列即為不存在**——白名單而非黑名單,
#: 因為黑名單漏一個就洩漏答案,白名單漏一個只是少給資訊。
CARD_FIELDS = ("card_id", "code", "name_today", "market", "industry", "limit_era",
               "era_code", "d_prev", "d0", "ret20", "ret60", "ret120", "adv_decile",
               "mom_decile", "pct_below_250d_high", "size_tier")

#: 年代分桶:提示詞要求「同批同年代」——年代語彙表(當年的題材用語)批內共用只採一次,
#: 而語彙每隔數年就換一輪,故以 2-3 年為一桶,並讓桶不跨越 2015-06 的漲跌幅世代斷點。
#: 2026-08-05 自查:原本 (2014, 2015) **跨越 2015-06-01 的漲跌幅世代斷點**,與註解
#: 自相矛盾,守衛實測即紅。改用日期區間(非年份)使桶界精確落在斷點上。
ERA_BANDS = (("2008-01-01", "2010-12-31"), ("2011-01-01", "2013-12-31"),
             ("2014-01-01", "2015-05-31"),          # 7% 世代結束於 2015-05-31
             ("2015-06-01", "2017-12-31"), ("2018-01-01", "2019-12-31"),
             ("2020-01-01", "2021-12-31"))


def _era_labels(seed: int) -> dict[tuple[str, str], str]:
    """把年代桶對應到**密封**期別碼 E1..E6。

    標籤刻意**打亂**再指派:若 E1..E6 依時序遞增,任何看到卡片的人(含蒸餾器)都能
    由代碼推回年代,密封就形同虛設——而期別保留組的整個隔離都建立在「蒸餾器不知道
    自己在看哪個年代」之上。對照表另存 `_era_map.json`,不進卡片、不給任何 agent。
    """
    labels = [f"E{i}" for i in range(1, len(ERA_BANDS) + 1)]
    random.Random(seed).shuffle(labels)
    return dict(zip(ERA_BANDS, labels))


def _era_band(d: Date) -> tuple[str, str]:
    """落不進任何桶就**當場炸**,不回 None。

    回 None 的話,`era_of[None]` 只會拋一個沒有上下文的 KeyError;更糟的是若哪天
    有人補了 `.get()`,期別碼就變成空字串,而空字串在密封期別的世界裡等於「所有
    卡片同一期」——保留組隔離無聲失效。
    """
    s = d.isoformat()
    for lo, hi in ERA_BANDS:
        if lo <= s <= hi:
            return (lo, hi)
    raise ValueError(f"{s} 不在任何年代桶內(ERA_BANDS 未覆蓋到樣本期間)")


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
    """代碼 → **最新申報的**公司名。

    不可用 `any_value()`:DuckDB 不保證它取同一列,兩次執行會拿到不同名稱
    (實測「德宏工業」vs「德宏」),卡片因此不可重現——而卡片不可重現就等於
    無法事後證明「當時給 agent 看的到底是什麼」,稽核鏈斷掉。
    `arg_max(name, 年月)` 既是決定性的,也正好符合 `name_today`(今天的名字)的語義。
    """
    q = con.sql("SELECT company_code, arg_max(company_name, year * 100 + month) AS nm "
                "FROM operating_revenue GROUP BY company_code").pl()
    return {r[C]: r["nm"] for r in q.iter_rows(named=True)}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--pilot", action="store_true",
                    help="試跑模式:**每個年代桶各出 1 批**(不是全域取前 N 批)。"
                         "試跑的唯一目的是量各年代的消息可得性,全押同一年代等於白跑")
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
    era_of = _era_labels(a.seed)
    _alias_seq = itertools.count(1)

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
            "era_code": era_of[_era_band(d0)],
            "d_prev": str(prev_of.get(d0, "")), "d0": str(d0),
            "ret20": f["ret20"], "ret60": f["ret60"], "ret120": f["ret120"],
            "adv_decile": row["adv_dec"], "mom_decile": row["mom_dec"],
            "pct_below_250d_high": f["pct_below_250d_high"],
            "size_tier": f["size_q"],
        }
        assert set(c) <= set(CARD_FIELDS), "卡片出現白名單外的欄位"
        # 蒸餾用假名:S=正例、C=near_miss 負例、Q=quiet 負例(提示詞的機制卡命名契約)。
        # 假名只進 truth 與日後的 mechanism_card,**絕不進階段 A 的卡片**——首字母就是答案。
        pfx = "S" if arm == "positive" else ("C" if row.get("neg_kind") == "near_miss" else "Q")
        alias = f"{pfx}{next(_alias_seq):03d}"
        truth = {"card_id": cid, "alias": alias, "code": row[C], "d0": str(d0), "arm": arm,
                 "fwd_max_ret": row["fwd_max_ret"], "regime": row["regime"],
                 "era_code": era_of[_era_band(d0)],
                 "neg_kind": row.get("neg_kind"), "matched_to": row.get("matched_to")}
        return c, truth

    # 配對關係:負例的 matched_to 指向正例;同批放入 2 組配對 + 2 檔未配對
    by_match = {r["matched_to"]: r for r in neg.to_dicts() if r.get("matched_to")}
    neg_rows = neg.to_dicts()
    # **按年代桶分群後才組批**:提示詞要求同批同年代(年代語彙表批內共用只採一次)。
    # 不分群的話一批會橫跨 3-4 個年份與兩個漲跌幅世代,語彙表共用機制當場失效。
    pos_by_era: dict[tuple[str, str], list[dict]] = {}
    for r in pos.to_dicts():
        b = _era_band(Date.fromisoformat(str(r["date"])))
        if b:
            pos_by_era.setdefault(b, []).append(r)
    used_neg: set[str] = set()
    batches: list[list[tuple[dict, dict]]] = []
    neg_by_era: dict[tuple[str, str], list[dict]] = {}
    for r in neg_rows:
        b = _era_band(Date.fromisoformat(str(r["date"])))
        if b:
            neg_by_era.setdefault(b, []).append(r)

    for band in sorted(pos_by_era):
        n_before = len(batches)
        pos_rows = pos_by_era[band][:]
        spare_neg = [r for r in neg_by_era.get(band, [])]
        rng.shuffle(pos_rows)
        rng.shuffle(spare_neg)
        i = 0
        while i + 2 <= len(pos_rows):
            items: list[tuple[dict, dict]] = []
            for p_row in pos_rows[i:i + 2]:                 # 2 組配對
                key = f"{p_row[C]}@{p_row['date']}"
                n_row = by_match.get(key)
                if not n_row or key in used_neg:
                    continue
                used_neg.add(key)
                for row, arm in ((p_row, "positive"), (n_row, "negative")):
                    got = card(row, arm, f"X{len(batches):03d}{len(items):02d}")
                    if got:
                        items.append(got)
            i += 2
            # 2 檔未配對(**同年代桶內**取),讓正負比例在 2:4 / 3:3 / 4:2 間浮動,
            # agent 無法由 3:3 的整齊結構反推出「這批是配對來的」
            pool = [(r, "positive") for r in pos_rows[i:i + 3]]
            pool += [(r, "negative") for r in spare_neg
                     if f"{r[C]}@{r['date']}" not in used_neg][:3]
            rng.shuffle(pool)
            for row, arm in pool[:2]:
                got = card(row, arm, f"X{len(batches):03d}{len(items):02d}")
                if got:
                    items.append(got)
            if len(items) < BATCH:
                continue
            rng.shuffle(items)
            batches.append(items[:BATCH])
            if a.pilot and len(batches) - n_before >= 1:
                break            # 每個年代桶只取 1 批,讓試跑橫跨全部年代

    OUT.mkdir(parents=True, exist_ok=True)
    TRUTH.mkdir(parents=True, exist_ok=True)
    npos = nneg = 0
    for bi, items in enumerate(batches):
        bid = f"batch_{bi:03d}"
        bdir = OUT / bid
        bdir.mkdir(exist_ok=True)
        cards = [c for c, _ in items]
        truth = [t for _, t in items]
        npos += sum(1 for t in truth if t["arm"] == "positive")
        nneg += sum(1 for t in truth if t["arm"] == "negative")
        (bdir / "cards.json").write_text(
            json.dumps(cards, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        (TRUTH / f"{bid}.json").write_text(          # **另一個根**,見模組 docstring
            json.dumps(truth, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    # 硬性驗證:卡片絕不可含任何結果欄位
    leak = [k for c in (json.loads((OUT / "batch_000" / "cards.json").read_text()))
            for k in c if k not in CARD_FIELDS]
    assert not leak, f"卡片洩漏結果欄位:{leak}"
    print(f"{len(batches)} 批 × {BATCH} 檔 = {len(batches)*BATCH} 張卡"
          f"(正 {npos} / 負 {nneg});每批正例數分布:"
          f"{sorted({sum(1 for _,t in b if t['arm']=='positive') for b in batches})}")
    print(f"卡片欄位:{list(json.loads((OUT/'batch_000'/'cards.json').read_text())[0])}")
    (OUT / "_era_map.json").write_text(json.dumps(
        {v: f"{k[0]}~{k[1]}" for k, v in era_of.items()}, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8")
    print(f"期別對照(**不給任何 agent**,僅供事後稽核)→ {OUT}/_era_map.json")
    print(f"→ {OUT}")


if __name__ == "__main__":
    main()
