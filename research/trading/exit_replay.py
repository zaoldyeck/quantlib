"""出場規則逐日重放——用價格路徑判定,不用今日快照。

**為什麼必須重放**(2026-07-16 使用者定調):

> 「我就算延遲了,該賣還是得賣,不能過時間了就當作沒發生。」

出場規則是**路徑相依**的:回測逐交易日評估,規則一觸發當天就出場。live 若只看
「今天的價格 vs 今天的止損線」,就會把「你沒跑報告那幾天已經觸發的出場」變成
沒發生過——@168 買進、第 3 天跌到 140(絕對停損 142.8 已破)、第 8 天反彈 170,
快照說「續抱」,**規則說「第 3 天就該賣」**。快照評估等於偷偷放寬規則,而且只在
「你沒天天看盤」時放寬——最沒紀律的時候最寬鬆,方向完全相反。

峰值(trailing 的錨)也一樣:先前是「跑報告時才更新」的增量值,漏跑幾天就漏掉
期間高點 → 止損線偏低 → 該賣的沒賣。這裡一律**由價格歷史重算**,與跑不跑無關。

本模組回傳「第一次觸發」的日期/價格/理由;呼叫端據此顯示逾期出場警示。
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Callable

import polars as pl

REPO_ROOT = Path(__file__).resolve().parents[2]
CACHE_DB = REPO_ROOT / "research" / "cache.duckdb"
FIRST_SEEN = REPO_ROOT / "research" / "data" / "revenue_first_seen.parquet"


@dataclass(frozen=True)
class DayState:
    """重放到某一交易日時,規則能看到的全部東西(全部 PIT)。"""
    day: date
    px: float
    peak: float          # 進場日以來的最高收盤(含當日)
    trough: float        # 進場日以來的最低收盤(含當日)
    days_held: int       # 交易日
    inst20: float | None  # 近 20 日法人淨買賣(股);None = 當日尚無資料
    yoy3: float | None    # 近三月營收 YoY 均值(%),PIT(依首見日)
    fresh_days: int | None  # 距最近一次月營收公布的日曆天數(S 的訊號新鮮度)


@dataclass(frozen=True)
class ExitFire:
    day: date
    reason: str
    price: float
    detail: str

    def is_overdue(self, today: date) -> bool:
        return self.day < today


def _con():
    import duckdb

    return duckdb.connect(str(CACHE_DB), read_only=True)


def load_paths(codes: list[str], start: date, end: date) -> dict[str, pl.DataFrame]:
    """每檔一張逐日表(close / inst20 / yoy3 / fresh_days),全部 PIT。

    - inst20:近 20 個交易日法人淨額滾動和(與 daily.py 的 live 語義同源)。
    - yoy3:近三月營收 YoY 均值,**以首見日(revenue_first_seen)決定何時可用**
      ——營收是事件驅動的,公布當天才准進決策。
    - fresh_days:距最近一次可用月營收的日曆天數(S 的訊號過期門用)。
    """
    if not codes:
        return {}
    con = _con()
    try:
        ph = ",".join("?" * len(codes))
        px = con.execute(
            f"SELECT company_code, date, any_value(closing_price) AS closing_price "
            f"FROM daily_quote WHERE company_code IN ({ph}) AND date BETWEEN ? AND ? "
            f"GROUP BY company_code, date ORDER BY company_code, date",  # 防同代碼跨市場重複列
            [*codes, start, end],
        ).pl()
        flows = con.execute(
            f"SELECT company_code, date, "
            f"coalesce(foreign_investors_difference,0) + coalesce(trust_difference,0) AS inst "
            f"FROM daily_trading_details WHERE company_code IN ({ph}) AND date <= ? "
            f"ORDER BY company_code, date",
            [*codes, end],
        ).pl()
        rev = con.execute(
            f"SELECT company_code, year, month, monthly_revenue_yoy FROM operating_revenue "
            f"WHERE company_code IN ({ph}) ORDER BY company_code, year, month",
            list(codes),
        ).pl()
    finally:
        con.close()

    inst20 = (flows.sort(["company_code", "date"])
              .with_columns(pl.col("inst").rolling_sum(20, min_periods=5)
                            .over("company_code").alias("inst20"))
              .select(["company_code", "date", "inst20"]))

    # 營收 PIT:首見日 = 可用日;沒有首見日紀錄的舊資料退回法定 10 日語義
    seen = (pl.read_parquet(FIRST_SEEN)
            .with_columns(pl.col("first_seen").str.to_date().alias("avail"))
            .select(["company_code", "year", "month", "avail"])
            if FIRST_SEEN.exists() else None)
    rev = rev.with_columns(
        pl.date(pl.col("year") + pl.col("month") // 12, pl.col("month") % 12 + 1, 10).alias("legal_avail"))
    if seen is not None:
        rev = (rev.join(seen, on=["company_code", "year", "month"], how="left")
               .with_columns(pl.coalesce([pl.col("avail"), pl.col("legal_avail")]).alias("avail")))
    else:
        rev = rev.with_columns(pl.col("legal_avail").alias("avail"))
    rev = (rev.sort(["company_code", "year", "month"])
           .with_columns(pl.col("monthly_revenue_yoy").rolling_mean(3, min_periods=2)
                         .over("company_code").alias("yoy3"))
           .select(["company_code", "avail", "yoy3"]).drop_nulls().sort("avail"))

    out: dict[str, pl.DataFrame] = {}
    for code in codes:
        p = px.filter(pl.col("company_code") == code).sort("date")
        if p.is_empty():
            continue
        # 鐵律:polars 的 join **不保證列序**,而 cum_max / join_asof 都吃順序
        # ——不重新 sort 就會在亂序上算峰值(2026-07-16 實測:2408 峰值算成 505,
        # 真值 481;台燿被誤判成觸發絕對停損)。每次 join 後一律重新 sort。
        p = p.join(inst20.filter(pl.col("company_code") == code).drop("company_code"),
                   on="date", how="left").sort("date")
        p = (p.join_asof(rev.filter(pl.col("company_code") == code).drop("company_code"),
                         left_on="date", right_on="avail", strategy="backward")
             .sort("date")
             .with_columns(
                 (pl.col("date") - pl.col("avail")).dt.total_days().alias("fresh_days")))
        # 注意:peak/trough **不能在這裡算**——它們是「進場日以來」的路徑量,
        # 每個 lot 的進場日不同。在載入窗上算 cum_max 會把進場前的高點算進去,
        # trailing 就會用一個根本不屬於這筆部位的峰值誤判出場(2026-07-16 實測:
        # 6446 峰值被算成 1530,那是 6 月的高點,進場後最高只有 1285)。
        out[code] = p
    return out


def replay(path: pl.DataFrame, entry_day: date, rule: Callable[[DayState], str | None],
           peak_floor: float | None = None) -> tuple[ExitFire | None, DayState | None]:
    """逐日重放,回傳 (第一次觸發, 今日狀態)。

    - 進場日當天不評估——回測語意是「T 日收盤買進,T+1 起受規則管束」。
    - `peak_floor` = 該筆的成交價:回測裡 `peak_close` 由 `entry_close`(即買進價)
      起算,故手動成交價高於當日收盤時,峰值下限應為成交價,trailing 才不會比
      回測寬鬆。
    """
    rows = (path.filter(pl.col("date") >= entry_day).sort("date")
            .with_columns([  # 峰/谷:只從這筆部位的進場日起算
                pl.col("closing_price").cum_max().alias("peak"),
                pl.col("closing_price").cum_min().alias("trough"),
            ]))
    if peak_floor:
        rows = rows.with_columns(
            pl.max_horizontal(pl.col("peak"), pl.lit(float(peak_floor))).alias("peak"))
    if rows.is_empty():
        return None, None
    fire: ExitFire | None = None
    last: DayState | None = None
    held = 0
    for r in rows.iter_rows(named=True):
        day = r["date"]
        st = DayState(
            day=day, px=float(r["closing_price"]), peak=float(r["peak"]),
            trough=float(r["trough"]), days_held=held,
            inst20=float(r["inst20"]) if r.get("inst20") is not None else None,
            yoy3=float(r["yoy3"]) if r.get("yoy3") is not None else None,
            fresh_days=int(r["fresh_days"]) if r.get("fresh_days") is not None else None,
        )
        last = st
        if day > entry_day and fire is None:
            reason = rule(st)
            if reason:
                fire = ExitFire(day=day, reason=reason, price=st.px,
                                detail=f"當時收 {st.px:g}(峰 {st.peak:g}、持有 {st.days_held} 日)")
        held += 1
    return fire, last


# ── 各策略的規則(常數來自各自的規格書,集中在此讓重放與報告共用一份) ──

def serenity_rule(anchor: float) -> Callable[[DayState], str | None]:
    """Serenity 六道門——規則源 `research/serenity/exit_rules.py`(與執行系統同一份)。"""
    from research.serenity.exit_rules import evaluate_exit

    def rule(st: DayState) -> str | None:
        return evaluate_exit(px=st.px, anchor=anchor, peak=st.peak,
                             days_held=st.days_held, inst20=st.inst20, yoy3=st.yoy3)
    return rule


def s_rule(cost: float | None) -> Callable[[DayState], str | None]:
    """S(apex_revcycle_S)——規格 `research/apex/STRATEGY.md`:
    訊號過期 26 日曆日 / trail 35% / 時間止損 30 交易日 / 輸家止損(水下且 ≥15 交易日)。"""
    def rule(st: DayState) -> str | None:
        if st.fresh_days is not None and st.fresh_days >= 26:
            return f"訊號過期(揭露後 {st.fresh_days} 日 ≥26)"
        if st.px <= st.peak * 0.65:
            return f"移動停損(自峰值 {st.peak:g} 回落 ≥35%)"
        if st.days_held >= 30:
            return f"時間止損(持有 {st.days_held} 交易日 ≥30)"
        if cost and st.px < cost and st.days_held >= 15:
            return f"輸家時間止損(水下且持有 {st.days_held} ≥15)"
        return None
    return rule


def evergreen_rule(cost: float | None, trail: float, lts: int) -> Callable[[DayState], str | None]:
    """Evergreen——參數來自 `research/evergreen/data/live_config.json`(EV43 年度 refit)。
    池籍到期是日曆規則、不是路徑規則,由呼叫端處理。"""
    def rule(st: DayState) -> str | None:
        if st.px <= st.peak * (1 - trail):
            return f"移動停損(自峰值 {st.peak:g} 回落 ≥{trail:.0%})"
        if cost and st.px < cost and st.days_held >= lts:
            return f"輸家時間止損(水下且持有 {st.days_held} ≥{lts})"
        return None
    return rule
