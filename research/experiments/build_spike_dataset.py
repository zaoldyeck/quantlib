"""Build spike event study dataset — 10 pre-window features + 3 forward returns.

Reads `research/out/spikes_g80_w60.parquet` (produced by 01_find_spikes.py).
For each spike event (T=spike_date, company=C), computes:

Pre-window features (PIT-safe, use only data with date <= T-1):
  1. revenue_yoy_3m_avg        — avg YoY of last 3 available monthly revenues
  2. revenue_accel             — latest YoY − 3m-avg YoY
  3. institutional_flow_20d    — (foreign + trust) net buy sum over T-20..T-1
  4. volume_surge_60d          — tv_avg(T-5..T-1) / tv_avg(T-60..T-1)
  5. margin_change_20d         — margin_balance(T-1) / margin_balance(T-20) − 1
  6. short_squeeze_proxy       — rank of short_balance(T-1) in T-60..T-1 (0..1)
  7. pre_breakout_consolidation — (max-min)/mean of close over T-60..T-1
  8. rsv_60d                   — RSV(close_T-1, min_60d, max_60d)
  9. near_52w_high             — close_T-1 / max(close, T-252..T-1)
  10. peer_relative_strength   — rank of 60d raw return in same industry

Forward returns (realised, T..T+N raw close ratio):
  - fwd_ret_5d, fwd_ret_21d, fwd_ret_63d (trading-day based)

Output: `research/experiments/spike_dataset.parquet`

Usage:
    uv run --project research python research/experiments/build_spike_dataset.py

Depends on cache.duckdb (needs daily_quote, daily_trading_details,
margin_transactions, operating_revenue). Run cache_tables.py first if missing.
"""
from __future__ import annotations

import os
import sys
import time
import polars as pl

# Make sibling db.py importable
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from db import connect  # noqa: E402

SPIKES_PATH = "research/out/spikes_g80_w60.parquet"
OUT_PATH = "research/experiments/spike_dataset.parquet"


def build_price_features(con, spikes: pl.DataFrame) -> pl.DataFrame:
    """Two-anchor feature build.

    Anchor A = start_date (row `date`): rally ENTRY. Used for pre-rally
    prediction features (Phase 2) and rally-progress reference returns.

    Anchor B = peak_date (= start_date + 60 trading days): rally EXIT.
    Used for post-peak drift returns (Phase 3b continuation study).

    Emits: peak_date, peak_close, pre-window aggregates (T-60..T-1 from
    start_date), rally_ret_{5,21,63}d (from start_date), and
    post_peak_ret_{5,21,63}d (from peak_date).
    """
    con.register("spikes_in", spikes.select(["company_code", "date", "closing_price"]))

    q = """
    WITH ranked AS (
      -- Trading-day row-number per company. Must filter closing_price > 0 to
      -- match find_spikes.py's row numbering (null/suspended days excluded).
      SELECT company_code, date, closing_price, trade_value,
             ROW_NUMBER() OVER (PARTITION BY company_code ORDER BY date) AS rn
      FROM daily_quote
      WHERE company_code IN (SELECT DISTINCT company_code FROM spikes_in)
        AND closing_price > 0
    ),
    events AS (
      -- Anchor each spike entry to its trading-day row number.
      SELECT s.company_code, s.date AS start_date, s.closing_price AS start_close,
             r.rn AS rn_start
      FROM spikes_in s
      JOIN ranked r
        ON r.company_code = s.company_code AND r.date = s.date
    )
    SELECT
      e.company_code, e.start_date, e.start_close,
      -- Peak reference (T+60 trading days)
      MAX(r.date)           FILTER (WHERE r.rn = e.rn_start + 60) AS peak_date,
      MAX(r.closing_price)  FILTER (WHERE r.rn = e.rn_start + 60) AS peak_close,
      -- Pre-window aggregates (start T-60..T-1) — filter by offset from rn_start.
      AVG(r.trade_value)    FILTER (WHERE r.rn BETWEEN e.rn_start - 60 AND e.rn_start - 1) AS tv_avg_60d,
      AVG(r.trade_value)    FILTER (WHERE r.rn BETWEEN e.rn_start - 5  AND e.rn_start - 1) AS tv_avg_5d,
      MIN(r.closing_price)  FILTER (WHERE r.rn BETWEEN e.rn_start - 60 AND e.rn_start - 1) AS px_min_60d,
      MAX(r.closing_price)  FILTER (WHERE r.rn BETWEEN e.rn_start - 60 AND e.rn_start - 1) AS px_max_60d,
      AVG(r.closing_price)  FILTER (WHERE r.rn BETWEEN e.rn_start - 60 AND e.rn_start - 1) AS px_avg_60d,
      MAX(r.closing_price)  FILTER (WHERE r.rn BETWEEN e.rn_start - 252 AND e.rn_start - 1) AS px_max_252d,
      -- close at T-1 (most recent pre-event close relative to start_date)
      MAX(r.closing_price)  FILTER (WHERE r.rn = e.rn_start - 1) AS close_tm1,
      -- 60-day raw return ending at T-1 (for peer relative strength)
      MAX(r.closing_price)  FILTER (WHERE r.rn = e.rn_start - 1)
        / NULLIF(MAX(r.closing_price) FILTER (WHERE r.rn = e.rn_start - 60), 0) - 1 AS ret_60d_pre,
      -- Rally progress (T+N from start_date) — dominated by the 80% rally by construction.
      MAX(r.closing_price)  FILTER (WHERE r.rn = e.rn_start + 5)  / e.start_close - 1 AS rally_ret_5d,
      MAX(r.closing_price)  FILTER (WHERE r.rn = e.rn_start + 21) / e.start_close - 1 AS rally_ret_21d,
      MAX(r.closing_price)  FILTER (WHERE r.rn = e.rn_start + 63) / e.start_close - 1 AS rally_ret_63d,
      -- Post-peak drift (T+N from peak_date = rn_start+60) — Phase 3b signal.
      MAX(r.closing_price)  FILTER (WHERE r.rn = e.rn_start + 65)
        / NULLIF(MAX(r.closing_price) FILTER (WHERE r.rn = e.rn_start + 60), 0) - 1 AS post_peak_ret_5d,
      MAX(r.closing_price)  FILTER (WHERE r.rn = e.rn_start + 81)
        / NULLIF(MAX(r.closing_price) FILTER (WHERE r.rn = e.rn_start + 60), 0) - 1 AS post_peak_ret_21d,
      MAX(r.closing_price)  FILTER (WHERE r.rn = e.rn_start + 123)
        / NULLIF(MAX(r.closing_price) FILTER (WHERE r.rn = e.rn_start + 60), 0) - 1 AS post_peak_ret_63d
    FROM events e
    JOIN ranked r
      ON r.company_code = e.company_code
     AND r.rn BETWEEN e.rn_start - 252 AND e.rn_start + 123
    GROUP BY e.company_code, e.start_date, e.start_close, e.rn_start
    """
    raw = con.sql(q).pl()

    return raw.with_columns([
        (pl.col("tv_avg_5d") / pl.col("tv_avg_60d")).alias("volume_surge_60d"),
        ((pl.col("px_max_60d") - pl.col("px_min_60d")) / pl.col("px_avg_60d")).alias("pre_breakout_consolidation"),
        ((pl.col("close_tm1") - pl.col("px_min_60d"))
         / (pl.col("px_max_60d") - pl.col("px_min_60d"))).alias("rsv_60d"),
        (pl.col("close_tm1") / pl.col("px_max_252d")).alias("near_52w_high"),
    ]).select([
        "company_code", "start_date", "start_close", "peak_date", "peak_close",
        "ret_60d_pre",
        "volume_surge_60d", "pre_breakout_consolidation", "rsv_60d", "near_52w_high",
        "rally_ret_5d", "rally_ret_21d", "rally_ret_63d",
        "post_peak_ret_5d", "post_peak_ret_21d", "post_peak_ret_63d",
    ])


def build_institutional_features(con, spikes: pl.DataFrame) -> pl.DataFrame:
    """institutional_flow_20d — sum(foreign + trust) net-buy-shares over T-20..T-1."""
    con.register("spikes_in", spikes.select(["company_code", "date"]))
    q = """
    WITH ranked AS (
      SELECT company_code, date,
             COALESCE(foreign_investors_difference, 0) + COALESCE(trust_difference, 0) AS inst_diff,
             ROW_NUMBER() OVER (PARTITION BY company_code ORDER BY date) AS rn
      FROM daily_trading_details
      WHERE company_code IN (SELECT DISTINCT company_code FROM spikes_in)
    ),
    events AS (
      SELECT s.company_code, s.date AS spike_date, r.rn AS rn_event
      FROM spikes_in s
      JOIN ranked r ON r.company_code = s.company_code AND r.date = s.date
    )
    SELECT e.company_code, e.spike_date,
           SUM(r.inst_diff) FILTER (WHERE r.rn BETWEEN e.rn_event - 20 AND e.rn_event - 1) AS institutional_flow_20d
    FROM events e
    JOIN ranked r ON r.company_code = e.company_code
                 AND r.rn BETWEEN e.rn_event - 20 AND e.rn_event - 1
    GROUP BY e.company_code, e.spike_date
    """
    return con.sql(q).pl()


def build_margin_features(con, spikes: pl.DataFrame) -> pl.DataFrame:
    """margin_change_20d + short_squeeze_proxy (pct-rank of short_balance at T-1
    within T-60..T-1 window per event)."""
    con.register("spikes_in", spikes.select(["company_code", "date"]))
    q = """
    WITH ranked AS (
      SELECT company_code, date, margin_balance, short_balance,
             ROW_NUMBER() OVER (PARTITION BY company_code ORDER BY date) AS rn
      FROM margin_transactions
      WHERE company_code IN (SELECT DISTINCT company_code FROM spikes_in)
    ),
    events AS (
      SELECT s.company_code, s.date AS spike_date, r.rn AS rn_event
      FROM spikes_in s
      JOIN ranked r ON r.company_code = s.company_code AND r.date = s.date
    ),
    -- All T-60..T-1 rows per event, with PERCENT_RANK of short_balance per-event.
    window_rows AS (
      SELECT e.company_code, e.spike_date, e.rn_event,
             r.rn, r.margin_balance, r.short_balance,
             PERCENT_RANK() OVER (
               PARTITION BY e.company_code, e.spike_date
               ORDER BY r.short_balance NULLS FIRST
             ) AS short_pct
      FROM events e
      JOIN ranked r ON r.company_code = e.company_code
                   AND r.rn BETWEEN e.rn_event - 60 AND e.rn_event - 1
    )
    SELECT company_code, spike_date,
           (MAX(margin_balance) FILTER (WHERE rn = rn_event - 1)::DOUBLE
            / NULLIF(MAX(margin_balance) FILTER (WHERE rn = rn_event - 20), 0)) - 1
             AS margin_change_20d,
           MAX(short_pct) FILTER (WHERE rn = rn_event - 1) AS short_squeeze_proxy
    FROM window_rows
    GROUP BY company_code, spike_date
    """
    return con.sql(q).pl()


def build_revenue_features(con, spikes: pl.DataFrame) -> pl.DataFrame:
    """PIT-safe revenue features.

    Publication rule: monthly revenue for (year, month) is public on the 10th
    of the NEXT month. Wait until day 13 for safety (prevents look-ahead).
    So as of spike_date T, latest usable revenue is the most recent (year, month)
    whose publish_date + 13 <= T, i.e. publish_date <= T - 13 days.
    publish_date ≈ next-month-10th = make_date(year, month+1, 10)  (boundary handled).
    """
    con.register("spikes_in", spikes.select(["company_code", "date"]))
    q = """
    WITH rev_pit AS (
      SELECT company_code, year, month, monthly_revenue_yoy AS yoy,
             -- PIT publish date: 10th of next month
             make_date(
               CASE WHEN month = 12 THEN year + 1 ELSE year END,
               CASE WHEN month = 12 THEN 1 ELSE month + 1 END,
               10
             ) AS pub_date
      FROM operating_revenue
      WHERE monthly_revenue_yoy IS NOT NULL
    ),
    -- For each event, rank usable revenue by pub_date DESC; take latest 3.
    matched AS (
      SELECT s.company_code, s.date AS spike_date,
             rp.yoy,
             ROW_NUMBER() OVER (
               PARTITION BY s.company_code, s.date
               ORDER BY rp.pub_date DESC
             ) AS rk
      FROM spikes_in s
      JOIN rev_pit rp
        ON rp.company_code = s.company_code
       AND rp.pub_date <= s.date - INTERVAL '3 days'  -- 10th + 3 = 13th safety buffer
    )
    SELECT company_code, spike_date,
           AVG(yoy) FILTER (WHERE rk = 1) AS revenue_yoy_latest,
           AVG(yoy) FILTER (WHERE rk BETWEEN 1 AND 3) AS revenue_yoy_3m_avg,
           (AVG(yoy) FILTER (WHERE rk = 1)
            - AVG(yoy) FILTER (WHERE rk BETWEEN 1 AND 3)) AS revenue_accel
    FROM matched
    WHERE rk <= 3
    GROUP BY company_code, spike_date
    """
    return con.sql(q).pl()


def build_peer_relative_strength(con, spikes: pl.DataFrame, price_feats: pl.DataFrame) -> pl.DataFrame:
    """peer_relative_strength = percentile-rank of ret_60d_pre within same industry
    at same spike_date (cross-sectional, among all stocks trading that day).

    Industry from operating_revenue (stock_per_pbr has no industry; operating_revenue
    does — take most-recent industry record per company).
    """
    # Most-recent industry per company
    ind = con.sql("""
      SELECT DISTINCT ON (company_code) company_code, industry
      FROM operating_revenue WHERE industry IS NOT NULL
      ORDER BY company_code, year DESC, month DESC
    """).pl()

    # Compute 60-day pre-window return for EVERY trading day for every spike company
    # — that's cross-sectional universe. We only need peer return at spike_date.
    # Simpler: for each spike event, get all stocks' 60d returns ending on spike_date,
    # filter by industry, rank the target.
    con.register("spikes_in", spikes.select(["company_code", "date"]))
    con.register("industry_map", ind)

    # 60-day pre-window raw return for every (company_code, date) — as-of event only.
    q = """
    WITH ranked AS (
      SELECT company_code, date, closing_price,
             ROW_NUMBER() OVER (PARTITION BY company_code ORDER BY date) AS rn
      FROM daily_quote
      WHERE regexp_matches(company_code, '^[1-9][0-9]{3}$')
    ),
    -- For each spike_date, compute 60d return of ALL stocks' T-60..T close.
    -- (Peer group = stocks in same industry trading that day.)
    all_ret AS (
      SELECT DISTINCT ON (r.company_code, s.date) s.date AS spike_date,
             r.company_code,
             (r.closing_price /
              NULLIF(
                (SELECT closing_price FROM ranked r2
                 WHERE r2.company_code = r.company_code
                   AND r2.rn = r.rn - 60), 0
              )) - 1 AS ret_60d
      FROM (SELECT DISTINCT date FROM spikes_in) s
      JOIN ranked r
        ON r.date = s.date
    )
    SELECT s.company_code, s.date AS spike_date,
           -- rank of spike-stock's ret_60d among same-industry peers on that spike_date
           (RANK() OVER (
              PARTITION BY s.date, im_self.industry
              ORDER BY ar_self.ret_60d
           ))::DOUBLE
           / NULLIF(COUNT(*) OVER (PARTITION BY s.date, im_self.industry), 0) AS peer_relative_strength
    FROM spikes_in s
    JOIN industry_map im_self ON im_self.company_code = s.company_code
    JOIN all_ret ar_self
      ON ar_self.company_code = s.company_code AND ar_self.spike_date = s.date
    JOIN all_ret ar_peers
      ON ar_peers.spike_date = s.date
    JOIN industry_map im_peers
      ON im_peers.company_code = ar_peers.company_code
     AND im_peers.industry = im_self.industry
    GROUP BY s.company_code, s.date, im_self.industry, ar_self.ret_60d
    """
    # The query above over-counts due to implicit cross product; redo cleaner:
    q2 = """
    WITH ranked AS (
      SELECT company_code, date, closing_price,
             ROW_NUMBER() OVER (PARTITION BY company_code ORDER BY date) AS rn
      FROM daily_quote
      WHERE regexp_matches(company_code, '^[1-9][0-9]{3}$')
        AND closing_price > 0
    ),
    all_ret AS (
      SELECT r.company_code, s.date AS spike_date,
             (r.closing_price / NULLIF(r2.closing_price, 0)) - 1 AS ret_60d
      FROM (SELECT DISTINCT date FROM spikes_in) s
      JOIN ranked r ON r.date = s.date
      LEFT JOIN ranked r2 ON r2.company_code = r.company_code AND r2.rn = r.rn - 60
    ),
    with_industry AS (
      SELECT ar.spike_date, ar.company_code, ar.ret_60d, im.industry
      FROM all_ret ar
      JOIN industry_map im ON im.company_code = ar.company_code
      WHERE ar.ret_60d IS NOT NULL AND im.industry IS NOT NULL
    ),
    ranked_peers AS (
      SELECT spike_date, company_code, industry, ret_60d,
             (PERCENT_RANK() OVER (PARTITION BY spike_date, industry ORDER BY ret_60d)) AS peer_pct
      FROM with_industry
    )
    SELECT s.company_code, s.date AS spike_date,
           rp.peer_pct AS peer_relative_strength
    FROM spikes_in s
    JOIN ranked_peers rp
      ON rp.company_code = s.company_code AND rp.spike_date = s.date
    """
    return con.sql(q2).pl()


def main():
    t0 = time.time()
    con = connect()
    print(f"[db] connected ({time.time()-t0:.2f}s)")

    spikes = pl.read_parquet(SPIKES_PATH)
    print(f"[in] {len(spikes)} spike events loaded from {SPIKES_PATH}")

    # Enrich with gain (already present in parquet)
    # Expected columns: company_code, date, closing_price, px_future, gain

    # 1. Price features + fwd returns
    t = time.time()
    price_feats = build_price_features(con, spikes)
    print(f"[1] price+fwd features: {price_feats.shape} ({time.time()-t:.2f}s)")

    # 2. Institutional flow
    t = time.time()
    inst_feats = build_institutional_features(con, spikes)
    print(f"[2] institutional features: {inst_feats.shape} ({time.time()-t:.2f}s)")

    # 3. Margin / short
    t = time.time()
    margin_feats = build_margin_features(con, spikes)
    print(f"[3] margin features: {margin_feats.shape} ({time.time()-t:.2f}s)")

    # 4. Revenue (PIT)
    t = time.time()
    rev_feats = build_revenue_features(con, spikes)
    print(f"[4] revenue features: {rev_feats.shape} ({time.time()-t:.2f}s)")

    # 5. Peer RS
    t = time.time()
    peer_feats = build_peer_relative_strength(con, spikes, price_feats)
    print(f"[5] peer RS: {peer_feats.shape} ({time.time()-t:.2f}s)")

    # 6. Merge all on (company_code, start_date). Sub-helpers still emit
    # `spike_date` — rename to start_date before join for consistency.
    base = spikes.rename({"date": "start_date"}).select(["company_code", "start_date", "gain"])
    for feats in (inst_feats, margin_feats, rev_feats, peer_feats):
        if "spike_date" in feats.columns:
            feats.rename({"spike_date": "start_date"}, strict=False)

    # Polars DataFrame.rename returns a new frame — re-bind.
    inst_feats = inst_feats.rename({"spike_date": "start_date"}) if "spike_date" in inst_feats.columns else inst_feats
    margin_feats = margin_feats.rename({"spike_date": "start_date"}) if "spike_date" in margin_feats.columns else margin_feats
    rev_feats = rev_feats.rename({"spike_date": "start_date"}) if "spike_date" in rev_feats.columns else rev_feats
    peer_feats = peer_feats.rename({"spike_date": "start_date"}) if "spike_date" in peer_feats.columns else peer_feats

    # Cast institutional_flow_20d Decimal → Float64 for cleaner downstream ML
    if "institutional_flow_20d" in inst_feats.columns:
        inst_feats = inst_feats.with_columns(
            pl.col("institutional_flow_20d").cast(pl.Float64, strict=False)
        )

    ds = (base
          .join(price_feats, on=["company_code", "start_date"], how="left")
          .join(inst_feats,  on=["company_code", "start_date"], how="left")
          .join(margin_feats, on=["company_code", "start_date"], how="left")
          .join(rev_feats,   on=["company_code", "start_date"], how="left")
          .join(peer_feats,  on=["company_code", "start_date"], how="left"))

    # Select final schema
    final_cols = [
        "company_code", "start_date", "peak_date", "gain",
        "start_close", "peak_close",
        # Pre-window features (for Phase 2 prediction — anchored at start_date)
        "revenue_yoy_latest", "revenue_yoy_3m_avg", "revenue_accel",
        "institutional_flow_20d",
        "volume_surge_60d",
        "margin_change_20d", "short_squeeze_proxy",
        "pre_breakout_consolidation", "rsv_60d", "near_52w_high",
        "peer_relative_strength",
        # Rally progress (anchored at start_date; dominated by spike definition — reference only)
        "rally_ret_5d", "rally_ret_21d", "rally_ret_63d",
        # Post-peak drift (anchored at peak_date — for Phase 3b chase-study)
        "post_peak_ret_5d", "post_peak_ret_21d", "post_peak_ret_63d",
    ]
    ds = ds.select([c for c in final_cols if c in ds.columns])

    os.makedirs(os.path.dirname(OUT_PATH), exist_ok=True)
    ds.write_parquet(OUT_PATH)

    print(f"\n[out] wrote {len(ds)} rows × {len(ds.columns)} cols → {OUT_PATH}")
    print(f"[out] total runtime: {time.time()-t0:.2f}s")
    print("\n=== Schema ===")
    print(ds.schema)
    print("\n=== Null counts per column ===")
    nulls = ds.null_count()
    for c in ds.columns:
        print(f"  {c:32} {nulls[c][0]:>5} / {len(ds)}")
    print("\n=== First 5 rows ===")
    with pl.Config(tbl_rows=5, tbl_cols=-1, tbl_width_chars=200, fmt_str_lengths=20):
        print(ds.head(5))
    print("\n=== Rally progress (from start_date — definition-dominated, reference only) ===")
    for col in ["rally_ret_5d", "rally_ret_21d", "rally_ret_63d"]:
        s = ds[col].drop_nulls()
        print(f"  {col:18} mean={s.mean():+.4f}  median={s.median():+.4f}  "
              f"std={s.std():.4f}  win_rate={(s > 0).sum()/len(s):.3f}  n={len(s)}")
    print("\n=== Post-peak drift (from peak_date = start + 60 trading days — Phase 3b key) ===")
    for col in ["post_peak_ret_5d", "post_peak_ret_21d", "post_peak_ret_63d"]:
        s = ds[col].drop_nulls()
        print(f"  {col:18} mean={s.mean():+.4f}  median={s.median():+.4f}  "
              f"std={s.std():.4f}  win_rate={(s > 0).sum()/len(s):.3f}  n={len(s)}")


if __name__ == "__main__":
    main()
