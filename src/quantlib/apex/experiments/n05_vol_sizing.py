"""N05 — 資金分配軸:波動加權 vs 等權(N04 條件機率地圖指出的唯一可動軸)。

**預註冊**:`ledger/batches.md` §N04/N05(2026-08-07),假設、臂、判準先寫後跑。

**假設來源(不是猜的)**:N04 地圖量到 S 的候選按進場波動分五層後,**風險調整後期望
單調**——E[未來 10 日報酬] ÷ σ 由低波到高波,IS 0.696/0.356/0.438/0.354/0.141、
OOS 0.945/0.019/0.330/0.404/0.024;裸期望值差異卻很小。白話:高波動候選賺得差不多,
卻用掉數倍風險。

**但這與 S 的血統相衝**:S 的超額來自微型股營收火箭的右尾(REPORT §8d),逆波動加權
會系統性減碼正是那些火箭。故本檔同時掛正波動加權臂(W4)當直球對決,**由資料裁決**。

**臂**(只改 `score_fn` 產出的 `weight` 欄 = 該倉目標 NAV 佔比,引擎零改動):
  A0 equal   等權 20%
  W1 shrink  0.20 × min(1, σ_ref/σ),clip [0.08, 0.20]  只縮高波、不加碼
  W2 parity  0.20 × (σ_ref/σ),      clip [0.10, 0.35]  完整逆波動
  W3 cut_hi  剔除當日 σ 最高五分位候選(池瘦身,對照 N03 的砍池成本 −13~−14pp)
  W4 lean    0.20 × (σ/σ_ref),      clip [0.10, 0.35]  正波動加權(右尾假說)

**判準**:CAGR 與 Sharpe 同時不劣於 A0 才算有價值;只升 CAGR 而 Sharpe/MDD 惡化 =
換到槓桿不是 alpha。勝出者須再過 IS/OOS 分窗一致性。

**PIT**:σ = 決策日(含)為止 20 日對數報酬標準差;σ_ref = 當日候選池 σ 中位(當下可知)。
引擎 ExecSpec 預設 fill_at="next_open",決策 t、成交 t+1,無前視。

依賴 cache: 是。
run: uv run --project . python -m quantlib.apex.experiments.n05_vol_sizing
"""
from __future__ import annotations

import polars as pl

from quantlib import paths
from quantlib.apex import data, metrics
from quantlib.apex.strategy_s import canonical_score, prep_cached, run_s_full

C = "company_code"
FULL = "2015-01-01"
SPLIT = "2021-07-01"          # 分窗一致性檢驗的切點(前後窗各約 5.5 年)
EQ_W = 0.20                   # 等權基準 = 1 / n_slots(PortSpec n_slots=5)
OUT = paths.OUT / "apex" / "n05_vol_sizing"


def sigma_table(panel: pl.DataFrame) -> pl.DataFrame:
    """(date, company_code, sig) — 20 日對數報酬標準差,決策日當下可知。"""
    return (panel.select(["date", C, "close"]).sort([C, "date"])
            .with_columns(
                (pl.col("close").log() - pl.col("close").log().shift(1)).over(C).alias("r"))
            .with_columns(pl.col("r").rolling_std(20).over(C).alias("sig"))
            .select(["date", C, "sig"])
            .filter(pl.col("sig") > 0))


def make_score_fn(sig: pl.DataFrame, mode: str):
    """canonical 分數不動,只加 `weight` 欄(或過濾候選)——差異可乾淨歸因到分配規則。"""
    def f(df: pl.DataFrame) -> pl.DataFrame:
        d = canonical_score(df).join(sig, on=["date", C], how="left")
        if mode == "equal":
            return d
        # σ 缺值(掛牌未滿 20 根)一律退回等權,不猜、不剔除
        ratio = pl.col("sig") / pl.col("sig").median().over("date")
        if mode == "cut_hi":
            q80 = pl.col("sig").quantile(0.8).over("date")
            return d.filter(pl.col("sig").is_null() | (pl.col("sig") <= q80))
        if mode == "shrink":
            w = (EQ_W * pl.min_horizontal(pl.lit(1.0), 1 / ratio)).clip(0.08, EQ_W)
        elif mode == "parity":
            w = (EQ_W / ratio).clip(0.10, 0.35)
        elif mode == "lean":
            w = (EQ_W * ratio).clip(0.10, 0.35)
        elif mode == "parity_norm":
            # 總曝險歸一的風險平價:權重在「當日榜上前 5 名」內正規化成合計 100%,
            # 才是真的把資金**移轉**而非**抽走**。W1/W2 只縮不放,測到的是降槓桿
            # (指紋:Sharpe 幾乎不動、CAGR 與 MDD 同比例縮小),不是風險平價本身。
            top = pl.col("score").rank("ordinal", descending=True).over("date") <= 5
            inv = pl.when(top).then(1 / pl.col("sig")).otherwise(None)
            w = (inv / inv.sum().over("date")).clip(0.05, 0.50)
        else:
            raise ValueError(mode)
        return d.with_columns(pl.when(pl.col("sig").is_null()).then(pl.lit(EQ_W))
                              .otherwise(w).alias("weight"))
    return f


ARMS = [
    ("A0 equal   等權 20%", "equal"),
    ("W1 shrink  只縮高波", "shrink"),
    ("W2 parity  完整逆波", "parity"),
    ("W3 cut_hi  剔高波五分位", "cut_hi"),
    ("W4 lean    正波動加權", "lean"),
    ("W5 parity_norm 曝險歸一逆波", "parity_norm"),
]


def _row(label: str, nav, trades) -> dict:
    st = metrics.summarize(nav, trades)
    return {"arm": label, "cagr": st["cagr"], "sharpe": st["sharpe"],
            "mdd": st["mdd"], "calmar": st["calmar"], "sortino": st["sortino"],
            "n_trades": st.get("n_trades", 0)}


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    con = data.connect()
    panel, feat, elig = prep_cached(con)
    sig = sigma_table(panel)
    print(f"σ 表 {sig.height:,} 列 | 全窗 start={FULL} | 分窗切點={SPLIT}\n")

    rows_full, rows_a, rows_b = [], [], []
    for label, mode in ARMS:
        fn = make_score_fn(sig, mode)
        nav, tr = run_s_full(panel, feat, elig, FULL, _score_fn=fn)
        rows_full.append(_row(label, nav, tr))
        # 分窗一致性:前窗 = 全窗 NAV 截到 SPLIT;後窗 = 由 SPLIT 重新起跑(乾淨起點)
        cut = pl.lit(SPLIT).str.to_date()
        na = nav.filter(pl.col("date") < cut)
        rows_a.append(_row(label, na.with_columns(pl.col("nav") / pl.col("nav").first()),
                           tr.filter(pl.col("entry_date") < cut)))
        nb, trb = run_s_full(panel, feat, elig, SPLIT, _score_fn=fn)
        rows_b.append(_row(label, nb, trb))
        nav.write_parquet(OUT / f"nav_{mode}.parquet")
        print(f"  {label} 完成")

    cols = ["arm", "cagr", "sharpe", "sortino", "mdd", "calmar", "n_trades"]
    for tag, rows in (("全窗 2015-2026", rows_full),
                      (f"前窗 2015~{SPLIT}", rows_a),
                      (f"後窗 {SPLIT}~2026", rows_b)):
        t = pl.DataFrame(rows)
        base = t.filter(pl.col("arm").str.starts_with("A0"))
        t = t.with_columns([
            ((pl.col("cagr") - base["cagr"][0]) * 100).round(2).alias("ΔCAGR_pp"),
            (pl.col("sharpe") - base["sharpe"][0]).round(3).alias("ΔSharpe"),
        ])
        print("\n" + "=" * 78)
        print(f"【{tag}】")
        print("=" * 78)
        with pl.Config(tbl_rows=-1, tbl_cols=-1, tbl_width_chars=170, float_precision=4):
            print(t.select(cols + ["ΔCAGR_pp", "ΔSharpe"]))
        t.write_parquet(OUT / f"arms_{tag.split()[0]}.parquet")
    print(f"\n產物 → {OUT}")


if __name__ == "__main__":
    main()
