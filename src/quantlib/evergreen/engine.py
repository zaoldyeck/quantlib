"""Evergreen 官方引擎(唯一真源)——資料 / 計分 / replay / refit / walk-forward。

**這是 Evergreen 回測的單一實作**。生產路徑(tri.pnl_dashboard、tri.daily)一律
呼叫本模組,禁止再手寫池籍/計分/simulate(2026-07-20 事故:dashboard 手寫
membership 偏離官方 midmonth_membership + 漏 gate,把 live-refit 線畫成一條
與 live_config 差 96pp 的降級版)。

血統對齊:資料構建與計分逐位對齊戰役檔 LabL(ev43)+ build_sc/nav_of(ev53);
`tests/test_engine_parity.py` 鎖死「本引擎重現 live_config 記錄的 316% train
CAGR」——重現 = 沒漂移,漂移即紅燈。只保留會影響 NAV 的欄位(h120/h52/adv20
+ gate 所需 inst5/f5/don60/rev_accel),丟掉 dead feature(mom 等)。

Run(自檢): uv run --project . python -m quantlib.evergreen.engine
依賴 cache: 是
"""
from __future__ import annotations

import itertools
import json
from dataclasses import dataclass, field
from datetime import date as Date
from pathlib import Path

import polars as pl

from quantlib.apex import data
from quantlib.apex.engine import ExecSpec, ExitSpec, PortSpec, simulate
from quantlib.evergreen.ev30_baseline import midmonth_membership
from quantlib.evergreen.ev36_walkforward import kpis_full, load_registry, seg_kpi
from quantlib import paths

C = "company_code"
LOAD_START = "2021-06-01"          # 暖機起點(與 LabX 一致,保 rolling 訊號逐位相同)
MEMB_FLOOR = Date(2022, 7, 11)     # 池 flag 起始(= 戰役 TRAIN0;registry 最早月)
LIVE_CONFIG = Path("src/quantlib/evergreen/data/live_config.json")

# 官方 refit 網格(EV38∪EV42 存活軸;變更須重過 walk-forward 驗證)
GATES = ("none", "f5", "inst5", "any_confirm")
SCORES = ("base", "xadv_inv")
_GRID_NUM = ((2, 3), (0.0, 0.6), (0.30, 0.40), (30, 45), (5, 6), (1, 2))
_TOPN_P5 = 40                      # P5(bootstrap)只算 top-Martin 前 N,控成本


def feat_cols() -> dict:
    """Evergreen 特徵表達式唯一真源(h120/h52/adv20/don60,over C)——EvergreenData
    與 tri.advisors 全用這份,禁止各自手寫(2026-07-20 消計分重複)。"""
    return {
        "h120": (pl.col("close") / pl.col("close").rolling_max(120)).over(C),
        "h52": (pl.col("close") / pl.col("close").rolling_max(252)).over(C),
        "adv20": pl.col("trade_value").cast(pl.Float64).rolling_median(20).over(C),
        "don60": (pl.col("close") > pl.col("close").shift(1).rolling_max(60)).over(C),
    }


def _rank(c: str) -> pl.Expr:
    return (pl.col(c).rank() / pl.len()).over("date")


def score_expr(cfg: dict) -> pl.Expr:
    """Evergreen 計分公式唯一真源(base = h52 rank × h120 rank;xadv_inv 再
    × (1 − adv20 rank))——engine.scores 與 tri.advisors 全用這份。"""
    base = _rank("h52") * _rank("h120")
    return base if cfg["score"] == "base" else base * (1.0 - _rank("adv20"))


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
        F = feat_cols()
        self.feats = (self.panel.with_columns(
            [F["h120"].alias("h120"), F["h52"].alias("h52")])
            .select(["date", C, "h120", "h52"]))
        self.trig = self._build_trig(codes)

    def _build_trig(self, codes: list[str]) -> pl.DataFrame:
        inst = pl.col("foreign_diff") + pl.col("trust_diff")
        fl = (data.load_flows(self.con, self.load_start, self.end)
              .filter(pl.col(C).is_in(codes)).sort([C, "date"])
              .with_columns([
                  (inst.rolling_sum(5).over(C) > 0).alias("inst5"),
                  (pl.col("foreign_diff").rolling_sum(5).over(C) > 0).alias("f5"),
              ]).select(["date", C, "inst5", "f5"]))
        F = feat_cols()
        px = (self.panel.with_columns(F["don60"].alias("don60"))
              .select(["date", C, "don60"]))
        rev = (data.load_monthly_revenue(self.con, self.end)
               .filter(pl.col(C).is_in(codes)).sort([C, "year", "month"])
               .with_columns([
                   pl.date(pl.col("year") + pl.col("month") // 12,
                           pl.col("month") % 12 + 1, 10).alias("avail"),
                   (pl.col("monthly_revenue_yoy")
                    > pl.col("monthly_revenue_yoy").shift(1).over(C)).alias("rev_accel"),
               ]).select([C, "avail", "rev_accel"])
               .drop_nulls(subset=["avail"]).sort("avail"))
        adv = (self.panel.sort([C, "date"]).with_columns(F["adv20"].alias("adv20"))
               .select(["date", C, "adv20"]))
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


def scores(d: EvergreenData, cfg: dict) -> tuple[pl.DataFrame, pl.DataFrame]:
    """cfg → (計分 sc, 池籍出場 flag)。與 ev53 build_sc 同語義。"""
    memb, pool_flag = d.memb(cfg["pool_months"])
    sc = (memb.join(d.feats, on=["date", C], how="left")
          .join(d.trig, on=["date", C], how="left")
          .filter(pl.col("h120").fill_null(0) > cfg["h120"]))
    if cfg["gate"] != "none":
        sc = sc.filter(pl.col(cfg["gate"]).fill_null(False))
    sc = (sc.with_columns(score_expr(cfg).alias("score"))
          .with_columns(pl.lit(1.0 / cfg["n_slots"]).alias("weight"))
          .select(["date", C, "score", "weight"]).drop_nulls()
          .sort(["date", "score", C], descending=[False, True, False]))
    return sc, pool_flag


def replay(d: EvergreenData, cfg: dict, start: Date, end: Date
           ) -> tuple[pl.DataFrame, pl.DataFrame]:
    """cfg 在 [start, end] 上的 (日 NAV, 交易明細)。trades = TRADE_SCHEMA:每筆
    進場日/出場日/ret_net(ROI)/days_held/exit_reason(open=期末未平倉=當下持有)。"""
    sc, pf = scores(d, cfg)
    res = simulate(d.panel.filter(pl.col("date") <= end), sc, exit_flags=pf,
                   exec_spec=ExecSpec(),
                   port_spec=PortSpec(n_slots=cfg["n_slots"],
                                      max_new_per_day=cfg["max_new"]),
                   exit_spec=ExitSpec(trailing_stop=cfg["trail"],
                                      abs_stop=cfg.get("abs_stop"),
                                      loser_time_stop=cfg["lts"]),
                   start=start)
    nav = res.nav.sort("date").filter(
        (pl.col("date") >= start) & (pl.col("date") <= end))
    trades = res.trades.filter(
        (pl.col("entry_date") >= start) & (pl.col("entry_date") <= end))
    return nav, trades


def replay_nav(d: EvergreenData, cfg: dict, start: Date, end: Date) -> pl.DataFrame:
    """replay 的 NAV-only 薄包裝(既有 ev53/parity 呼叫者相容)。"""
    return replay(d, cfg, start, end)[0]


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


_REFIT_CACHE = Path("src/quantlib/evergreen/data/_refit_cache.json")


def _refit_cached(d: EvergreenData, train_start: Date, train_end: Date) -> dict:
    """refit 磁碟快取(#3 增量,回應「不該全窗重放」):key = 引擎版本 + 訓練窗。
    fold0/1/2 訓練窗固定在過去、永不變 → 昂貴的網格搜尋只跑一次,新資料日只重放
    尾巴(~3-4 分 → ~30 秒)。cache 全表重建改動歷史價時,bump _ENGINE_VER 即失效。"""
    key = f"{_ENGINE_VER}|{train_start}|{train_end}"
    cache = json.loads(_REFIT_CACHE.read_text()) if _REFIT_CACHE.exists() else {}
    if key not in cache:
        cache[key] = refit(d, train_start, train_end)
        _REFIT_CACHE.write_text(json.dumps(cache, ensure_ascii=False, indent=1))
    return cache[key]


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


def walkforward(con, end: Date) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Evergreen 誠實前瞻 NAV 線 + 交易明細。**NAV** = 逐段 refit-on-past 拼 OOS
    (首年 in_sample、2023-07 起真走查,見 dashboard 虛實線)。**trades** = 現行 live
    參數的『連續』回放(當下持有 = 其 open 部位 = 正確的持續組合;交易行為視角,
    有別於走查 NAV 線,dashboard 附註標明)。"""
    d = EvergreenData(con, end.isoformat())
    segs = []
    for k, f in enumerate(_folds(end)):
        cfg = _refit_cached(d, *f["tr"]) if f["refit"] else _live_cfg()
        if k == 0:  # 初始 in-sample 段:fold0 config 套自己 train 窗 → 線從 2022-07 起
            si = replay_nav(d, cfg, f["tr"][0], f["tr"][1])
            if si.height:
                segs.append(si.with_columns([
                    (pl.col("nav") / pl.col("nav").first()).alias("r"),
                    pl.lit(True).alias("in_sample")]))
        o0, o1 = f["oos"]
        if o1 <= o0:
            continue
        nav_s = replay_nav(d, cfg, o0, o1)
        if nav_s.height:
            segs.append(nav_s.with_columns([
                (pl.col("nav") / pl.col("nav").first()).alias("r"),
                pl.lit(False).alias("in_sample")]))
    if not segs:
        return pl.DataFrame({"date": [], "nav": []}), pl.DataFrame()
    # 拼接:逐段複利,前段終值 × 後段歸一
    out, base = [], 1.0
    for s in segs:
        s = s.with_columns((pl.col("r") * base).alias("nav")).select(
            ["date", "nav", "in_sample"])
        out.append(s)
        base = float(s["nav"][-1])
    nav = pl.concat(out).unique(subset="date", keep="first").sort("date")
    _, trades = replay(d, _live_cfg(), MEMB_FLOOR, end)  # 交易 = live 參數連續回放
    return nav, trades


def walkforward_nav(con, end: Date, out_start: Date | None = None) -> pl.DataFrame:
    """walkforward 的 NAV-only 薄包裝(既有呼叫者相容)。"""
    nav = walkforward(con, end)[0]
    return nav.filter(pl.col("date") >= out_start) if out_start is not None else nav


_WF_CACHE = Path("src/quantlib/evergreen/data/_wf_nav_cache.parquet")
_CACHE_DB = Path(f"{paths.CACHE_DB}")
_ENGINE_VER = "ev53wf-5"  # 遞增即強制 wf 重算(-2 last-day、-3 移除 registry 硬切、-4 初始 in-sample 段、
                          #  -5 2026-07-24 資料權威重建〔raw 全量 rebuild+錯日/幽靈清除〕→ 全 fold refit 重選)


def _wf_key(end: Date) -> str:
    mt = _CACHE_DB.stat().st_mtime if _CACHE_DB.exists() else 0
    lc = LIVE_CONFIG.stat().st_mtime if LIVE_CONFIG.exists() else 0
    return f"{_ENGINE_VER}|{end}|{mt:.0f}|{lc:.0f}"


_WF_TRADES_CACHE = Path("src/quantlib/evergreen/data/_wf_trades_cache.parquet")


def walkforward_cached(con, end: Date) -> tuple[pl.DataFrame, pl.DataFrame]:
    """(nav, trades) 磁碟快取(key = 引擎版本 + cache/live_config mtime);資料世代
    一變即失效重算。dashboard/daily 走這條,避免每次出圖重跑網格 + 交易。"""
    key = _wf_key(end)
    if _WF_CACHE.exists() and _WF_TRADES_CACHE.exists():
        cn = pl.read_parquet(_WF_CACHE)
        if cn.height and cn["_key"][0] == key:
            ct = pl.read_parquet(_WF_TRADES_CACHE)
            return cn.drop("_key"), (ct.drop("_key") if "_key" in ct.columns else ct)
    nav, trades = walkforward(con, end)
    nav.with_columns(pl.lit(key).alias("_key")).write_parquet(_WF_CACHE)
    (trades.with_columns(pl.lit(key).alias("_key")) if trades.height
     else pl.DataFrame({"_key": [key]})).write_parquet(_WF_TRADES_CACHE)
    return nav, trades


def walkforward_nav_cached(con, end: Date, out_start: Date | None = None) -> pl.DataFrame:
    nav = walkforward_cached(con, end)[0]
    return nav.filter(pl.col("date") >= out_start) if out_start is not None else nav


def walkforward_trades_cached(con, end: Date) -> pl.DataFrame:
    return walkforward_cached(con, end)[1]


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
