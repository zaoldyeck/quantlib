"""Evergreen 官方引擎(唯一真源)——資料 / 計分 / replay / refit / walk-forward。

**這是 Evergreen 回測的單一實作**。生產路徑(tri.pnl_dashboard、tri.daily)一律
呼叫本模組,禁止再手寫池籍/計分/simulate(2026-07-20 事故:dashboard 手寫
membership 偏離官方 midmonth_membership + 漏 gate,把 live-refit 線畫成一條
與 live_config 差 96pp 的降級版)。

血統對齊:資料構建與計分逐位對齊戰役檔 LabL(ev43)+ build_sc/nav_of(ev53);
`tests/test_engine_parity.py` 鎖死「本引擎重現 live_config 記錄的 316% train
CAGR」——重現 = 沒漂移,漂移即紅燈。只保留會影響 NAV 的欄位(h120/h52/adv20
+ gate 所需 inst5/f5/don60/rev_accel),丟掉 dead feature(mom 等)。

Run(自檢): uv run --project research python -m research.evergreen.engine
依賴 cache: 是
"""
from __future__ import annotations

import itertools
import json
from dataclasses import dataclass, field
from datetime import date as Date
from pathlib import Path

import polars as pl

from research.apex import data
from research.apex.engine import ExecSpec, ExitSpec, PortSpec, simulate
from research.evergreen.ev30_baseline import midmonth_membership
from research.evergreen.ev36_walkforward import kpis_full, load_registry, seg_kpi

C = "company_code"
LOAD_START = "2021-06-01"          # 暖機起點(與 LabX 一致,保 rolling 訊號逐位相同)
MEMB_FLOOR = Date(2022, 7, 11)     # 池 flag 起始(= 戰役 TRAIN0;registry 最早月)
LIVE_CONFIG = Path("research/evergreen/data/live_config.json")

# 官方 refit 網格(EV38∪EV42 存活軸;變更須重過 walk-forward 驗證)
GATES = ("none", "f5", "inst5", "any_confirm")
SCORES = ("base", "xadv_inv")
_GRID_NUM = ((2, 3), (0.0, 0.6), (0.30, 0.40), (30, 45), (5, 6), (1, 2))
_TOPN_P5 = 40                      # P5(bootstrap)只算 top-Martin 前 N,控成本


# ───────────────────────── 資料層(唯一真源)─────────────────────────


@dataclass
class EvergreenData:
    """池股 panel + 計分特徵 + 籌碼/催化 trigger + 月中池籍。參數化 start/end,
    邏輯逐位對齊 LabL;只建會影響 NAV 的欄位。"""

    con: object
    end: str
    load_start: str = LOAD_START
    panel: pl.DataFrame = field(init=False)
    feats: pl.DataFrame = field(init=False)
    trig: pl.DataFrame = field(init=False)
    dates_all: list = field(init=False)
    reg: pl.DataFrame = field(init=False)
    _memb: dict = field(default_factory=dict, init=False)

    def __post_init__(self) -> None:
        self.reg = load_registry()
        codes = self.reg["code"].unique().to_list()
        panel_full = data.common_stocks(
            data.load_panel(self.con, self.load_start, self.end, warmup_days=300))
        self.dates_all = (panel_full.select("date").unique()
                          .sort("date")["date"].to_list())
        self.panel = panel_full.filter(pl.col(C).is_in(codes)).sort([C, "date"])
        self.feats = (self.panel.with_columns([
            (pl.col("close") / pl.col("close").rolling_max(120)).over(C).alias("h120"),
            (pl.col("close") / pl.col("close").rolling_max(252)).over(C).alias("h52"),
        ]).select(["date", C, "h120", "h52"]))
        self.trig = self._build_trig(codes)

    def _build_trig(self, codes: list[str]) -> pl.DataFrame:
        inst = pl.col("foreign_diff") + pl.col("trust_diff")
        fl = (data.load_flows(self.con, self.load_start, self.end)
              .filter(pl.col(C).is_in(codes)).sort([C, "date"])
              .with_columns([
                  (inst.rolling_sum(5).over(C) > 0).alias("inst5"),
                  (pl.col("foreign_diff").rolling_sum(5).over(C) > 0).alias("f5"),
              ]).select(["date", C, "inst5", "f5"]))
        px = (self.panel.with_columns(
            (pl.col("close") > pl.col("close").shift(1).rolling_max(60))
            .over(C).alias("don60")).select(["date", C, "don60"]))
        rev = (data.load_monthly_revenue(self.con, self.end)
               .filter(pl.col(C).is_in(codes)).sort([C, "year", "month"])
               .with_columns([
                   pl.date(pl.col("year") + pl.col("month") // 12,
                           pl.col("month") % 12 + 1, 10).alias("avail"),
                   (pl.col("monthly_revenue_yoy")
                    > pl.col("monthly_revenue_yoy").shift(1).over(C)).alias("rev_accel"),
               ]).select([C, "avail", "rev_accel"])
               .drop_nulls(subset=["avail"]).sort("avail"))
        adv = (self.panel.sort([C, "date"]).with_columns(
            pl.col("trade_value").cast(pl.Float64).rolling_median(20)
            .over(C).alias("adv20")).select(["date", C, "adv20"]))
        return (self.feats.select(["date", C])
                .join(fl, on=["date", C], how="left")
                .join(px, on=["date", C], how="left").sort("date")
                .join_asof(rev, left_on="date", right_on="avail", by=C,
                           strategy="backward", tolerance="70d")
                .join(adv, on=["date", C], how="left")
                .with_columns([pl.col(c).fill_null(False)
                               for c in ("inst5", "f5", "don60", "rev_accel")])
                .with_columns([
                    (pl.col("don60") | pl.col("inst5") | pl.col("rev_accel"))
                    .alias("any_confirm"),
                    pl.col("adv20").fill_null(1e12),
                ]))

    def memb(self, pool_months: int) -> tuple[pl.DataFrame, pl.DataFrame]:
        if pool_months not in self._memb:
            m = midmonth_membership(self.reg, self.dates_all, pool_months)
            days = [d for d in self.dates_all if d >= MEMB_FLOOR]
            flag = (pl.DataFrame({"date": days})
                    .join(pl.DataFrame({C: m[C].unique().to_list()}), how="cross")
                    .join(m.select(["date", C]), on=["date", C], how="anti")
                    .sort(["date", C]))
            self._memb[pool_months] = (m, flag)
        return self._memb[pool_months]


# ───────────────────────── 計分 + replay(= build_sc + nav_of)──────────


def _rank(c):
    return (pl.col(c).rank() / pl.len()).over("date")


def scores(d: EvergreenData, cfg: dict) -> tuple[pl.DataFrame, pl.DataFrame]:
    """cfg → (計分 sc, 池籍出場 flag)。與 ev53 build_sc 同語義。"""
    memb, pool_flag = d.memb(cfg["pool_months"])
    sc = (memb.join(d.feats, on=["date", C], how="left")
          .join(d.trig, on=["date", C], how="left")
          .filter(pl.col("h120").fill_null(0) > cfg["h120"]))
    if cfg["gate"] != "none":
        sc = sc.filter(pl.col(cfg["gate"]).fill_null(False))
    base = _rank("h52") * _rank("h120")
    expr = base if cfg["score"] == "base" else base * (1.0 - _rank("adv20"))
    sc = (sc.with_columns(expr.alias("score"))
          .with_columns(pl.lit(1.0 / cfg["n_slots"]).alias("weight"))
          .select(["date", C, "score", "weight"]).drop_nulls()
          .sort(["date", "score", C], descending=[False, True, False]))
    return sc, pool_flag


def replay_nav(d: EvergreenData, cfg: dict, start: Date, end: Date) -> pl.DataFrame:
    """cfg 在 [start, end] 上的日 NAV(fresh 起跑)。含選配 abs_stop / 曝險 overlay
    無關——純引擎。與 ev53 nav_of 逐位對齊。"""
    sc, pf = scores(d, cfg)
    res = simulate(d.panel.filter(pl.col("date") <= end), sc, exit_flags=pf,
                   exec_spec=ExecSpec(),
                   port_spec=PortSpec(n_slots=cfg["n_slots"],
                                      max_new_per_day=cfg["max_new"]),
                   exit_spec=ExitSpec(trailing_stop=cfg["trail"],
                                      abs_stop=cfg.get("abs_stop"),
                                      loser_time_stop=cfg["lts"]),
                   start=start)
    return res.nav.sort("date").filter(
        (pl.col("date") >= start) & (pl.col("date") <= end))


# ───────────────────────── refit(= ev43 網格 + P5 選型)────────────────


def refit(d: EvergreenData, train_start: Date, train_end: Date) -> dict:
    """train 窗上跑官方網格,MDD 無約束、max P5(EV44 主尺)→ 上場 cfg。
    = live_config 的產生方式;walk-forward 每段 refit 都走這條。"""
    grid = [dict(gate=g, score=s, pool_months=pm, h120=h, trail=tr, lts=lt,
                 n_slots=ns, max_new=mn)
            for g, s, (pm, h, tr, lt, ns, mn) in itertools.product(
                GATES, SCORES, itertools.product(*_GRID_NUM))]
    rows = []
    for cfg in grid:
        k = seg_kpi(replay_nav(d, cfg, train_start, train_end))
        rows.append({**cfg, "martin": k["martin"], "cagr": k["cagr"]})
    rows.sort(key=lambda r: -r["martin"])
    best = None
    for r in rows[:_TOPN_P5]:
        cfg = {k: r[k] for k in ("gate", "score", "pool_months", "h120",
                                 "trail", "lts", "n_slots", "max_new")}
        p5 = kpis_full(replay_nav(d, cfg, train_start, train_end))["p5"]
        if best is None or p5 > best[1]:
            best = (cfg, p5)
    return best[0]


# ───────────────────────── walk-forward(誠實前瞻曲線)────────────────


def _folds(end: Date) -> list[dict]:
    """滾動 refit 排程:年度 refit、擴張窗(registry 2022-07 起,資料有限時
    擴張至滿 3 年);每段 OOS 只用『該段訓練終點之前』選出的參數。最後一段
    forward 用實際上場的 live_config(而非重 refit),忠實反映部署現況。"""
    return [
        {"tr": (Date(2022, 7, 11), Date(2023, 7, 10)), "oos": (Date(2023, 7, 11), Date(2024, 7, 10)), "refit": True},
        {"tr": (Date(2022, 7, 11), Date(2024, 7, 10)), "oos": (Date(2024, 7, 11), Date(2025, 7, 10)), "refit": True},
        {"tr": (Date(2022, 7, 11), Date(2025, 7, 10)), "oos": (Date(2025, 7, 11), Date(2026, 7, 9)), "refit": True},
        {"tr": None, "oos": (Date(2026, 7, 10), end), "refit": False},  # forward = live_config
    ]


def _live_cfg() -> dict:
    return json.loads(LIVE_CONFIG.read_text())["config"]


def walkforward_nav(con, end: Date, out_start: Date | None = None) -> pl.DataFrame:
    """三策略儀表板用的 Evergreen 誠實前瞻線:逐段 refit-on-past → 拼 OOS。
    回 (date, nav) 連續曲線,起於首個 OOS(2023-07;之前是初訓期,無可交易軌跡)。"""
    d = EvergreenData(con, end.isoformat())
    segs = []
    for f in _folds(end):
        o0, o1 = f["oos"]
        if o1 <= o0:
            continue
        cfg = refit(d, *f["tr"]) if f["refit"] else _live_cfg()
        nav = replay_nav(d, cfg, o0, o1)
        if nav.height:
            segs.append(nav.with_columns(
                (pl.col("nav") / pl.col("nav").first()).alias("r")))
    if not segs:
        return pl.DataFrame({"date": [], "nav": []})
    # 拼接:逐段複利,前段終值 × 後段歸一
    out, base = [], 1.0
    for s in segs:
        s = s.with_columns((pl.col("r") * base).alias("nav")).select(["date", "nav"])
        out.append(s)
        base = float(s["nav"][-1])
    nav = pl.concat(out).unique(subset="date", keep="first").sort("date")
    if out_start is not None:
        nav = nav.filter(pl.col("date") >= out_start)
    return nav


_WF_CACHE = Path("research/evergreen/data/_wf_nav_cache.parquet")
_CACHE_DB = Path("research/cache.duckdb")
_ENGINE_VER = "ev53wf-1"  # 改引擎邏輯時遞增,強制 walk-forward 重算


def _wf_key(end: Date) -> str:
    mt = _CACHE_DB.stat().st_mtime if _CACHE_DB.exists() else 0
    lc = LIVE_CONFIG.stat().st_mtime if LIVE_CONFIG.exists() else 0
    return f"{_ENGINE_VER}|{end}|{mt:.0f}|{lc:.0f}"


def walkforward_nav_cached(con, end: Date, out_start: Date | None = None) -> pl.DataFrame:
    """walkforward_nav 的磁碟快取版(key = 引擎版本 + cache/live_config mtime);
    資料世代一變即失效重算。dashboard/daily 走這條,避免每次出圖重跑網格。"""
    key = _wf_key(end)
    if _WF_CACHE.exists():
        cached = pl.read_parquet(_WF_CACHE)
        if cached.height and cached["_key"][0] == key:
            nav = cached.drop("_key")
            return nav.filter(pl.col("date") >= out_start) if out_start else nav
    nav = walkforward_nav(con, end)
    nav.with_columns(pl.lit(key).alias("_key")).write_parquet(_WF_CACHE)
    return nav.filter(pl.col("date") >= out_start) if out_start else nav


def _selfcheck() -> None:
    """自檢:重現 live_config 記錄的 train CAGR(單一 config replay parity)。"""
    con = data.connect()
    d = EvergreenData(con, "2026-07-09")
    cfg = _live_cfg()
    doc = json.loads(LIVE_CONFIG.read_text())
    t0 = Date.fromisoformat(doc["train_window"][0])
    t1 = Date.fromisoformat(doc["train_window"][1])
    k = seg_kpi(replay_nav(d, cfg, t0, t1))
    rec = doc["train_kpi"]["cagr"]
    print(f"live_config train CAGR 記錄 {rec:.4f} / 引擎重現 {k['cagr']:.4f} "
          f"/ 差 {abs(k['cagr'] - rec):.2%}  "
          f"{'✓ PARITY' if abs(k['cagr'] - rec) < 0.005 else '✗ 漂移!'}")
    print(f"MDD 記錄 {doc['train_kpi']['mdd']:.4f} / 重現 {k['mdd']:.4f}")


def _wf_report() -> None:
    """walk-forward 誠實線 vs in-sample 全窗 replay(對照,量誠實代價)。"""
    con = data.connect()
    end = data.latest_date(con)
    wf = walkforward_nav_cached(con, end)
    import numpy as np
    v = wf["nav"].to_numpy()
    yrs = (wf["date"][-1] - wf["date"][0]).days / 365.25
    print(f"\nEvergreen walk-forward 誠實線 {wf['date'][0]}~{wf['date'][-1]}:")
    print(f"  {wf.height} 日;總報酬 {v[-1]:.2f}x;CAGR {v[-1] ** (1 / yrs) - 1:.1%};"
          f"MDD {(v / np.maximum.accumulate(v) - 1).min():.1%}")
    years = wf["date"].dt.year().to_numpy()
    for y in np.unique(years):
        idx = np.where(years == y)[0]
        prev = v[idx[0] - 1] if idx[0] > 0 else v[idx[0]]
        print(f"    {y}: {v[idx[-1]] / prev - 1:+.1%}")


if __name__ == "__main__":
    import sys
    _wf_report() if "--wf" in sys.argv else _selfcheck()
