"""DB-sourced active ETF analysis with dividend adjustment.

Apples-to-apples vs iter_21 (DRIP NAV). Method:
- Pull daily closing_price from pg.daily_quote
- Pull cash_dividend from pg.ex_right_dividend
- On ex-date, adjust pre-ex prices upward by (1 + div / pre_ex_close)
  → produces total-return series equivalent to DRIP held position
- Compute CAGR / Sharpe / Sortino / MDD / vol / beta / alpha vs 0050
"""
import polars as pl
import numpy as np
import duckdb
from datetime import date

con = duckdb.connect()
con.sql("INSTALL postgres; LOAD postgres;")
con.sql("ATTACH 'host=localhost port=5432 dbname=quantlib' AS pg (TYPE postgres, READ_ONLY)")

tickers = ['00981A','00982A','00984A','00987A','00988A','00990A','00991A',
           '00992A','00993A','00994A','00995A','0050','0052']
in_clause = ",".join(f"'{t}'" for t in tickers)

# Pull prices
prices = con.sql(f"""
    SELECT date, company_code, company_name, closing_price
    FROM pg.public.daily_quote
    WHERE market='twse' AND company_code IN ({in_clause})
""").pl()

# Pull dividends
divs = con.sql(f"""
    SELECT date AS ex_date, company_code, cash_dividend
    FROM pg.public.ex_right_dividend
    WHERE company_code IN ({in_clause})
      AND cash_dividend > 0
""").pl()
print(f"loaded {prices.height} prices, {divs.height} dividend events\n")


def total_return_series(asset_prices: pl.DataFrame, asset_divs: pl.DataFrame) -> pl.DataFrame:
    """Convert price + cash dividends into total-return adjusted close series.

    Using the standard 'reverse split factor' method:
      adj_close[t] = close[t] for t >= last_ex
      For ex-date d with div D and pre-ex close P_pre:
        scale = P_pre / (P_pre - D)    (adjustment factor pre-ex)
        adj_close[t < d] *= scale       (back-propagate)
    Result: adjusted series where buying at adjusted close[0] and not reinvesting
    gives the same total return as buying at close[0] and reinvesting all dividends.
    """
    s = asset_prices.sort("date").clone()
    s = s.with_columns(adj_close=pl.col("closing_price"))
    if asset_divs.height == 0:
        return s
    for row in asset_divs.sort("ex_date", descending=True).iter_rows(named=True):
        ex = row["ex_date"]; d = row["cash_dividend"]
        # find last close before ex
        pre = s.filter(pl.col("date") < ex).tail(1)
        if pre.height == 0: continue
        p_pre = pre["closing_price"][0]
        if p_pre <= d: continue
        # 降低 ex-date 之前的歷史價格，使整段 series 反映 hold + DRIP 的實際 total return.
        # scale = (P_pre - D) / P_pre  = post-ex price / pre-ex price (without market move)
        scale = (p_pre - d) / p_pre
        s = s.with_columns(
            adj_close=pl.when(pl.col("date") < ex).then(pl.col("adj_close") * scale).otherwise(pl.col("adj_close"))
        )
    return s


def metrics(prices_arr, dates):
    if len(prices_arr) < 5: return None
    rets = np.diff(prices_arr) / prices_arr[:-1]
    days = (dates[-1] - dates[0]).days
    yrs = days / 365.25
    cum = prices_arr[-1] / prices_arr[0] - 1
    cagr = (1 + cum) ** (1/yrs) - 1 if yrs > 0 else cum
    sharpe = rets.mean() / rets.std() * np.sqrt(252) if rets.std() > 0 else 0
    downside = rets[rets < 0]
    sortino = rets.mean() / downside.std() * np.sqrt(252) if len(downside) > 0 and downside.std() > 0 else 0
    peak = np.maximum.accumulate(prices_arr)
    mdd = float(np.min(prices_arr / peak - 1))
    vol = rets.std() * np.sqrt(252)
    return dict(days=days, yrs=yrs, cum=cum, cagr=cagr,
                sharpe=sharpe, sortino=sortino, mdd=mdd, vol=vol)


# 0050 dividend-adjusted benchmark
bench0050 = total_return_series(
    prices.filter(pl.col("company_code") == "0050"),
    divs.filter(pl.col("company_code") == "0050"),
)
print("=== 0050 全期 vs price-only sanity check ===")
m_price = metrics(bench0050["closing_price"].to_numpy(), bench0050["date"].to_list())
m_total = metrics(bench0050["adj_close"].to_numpy(), bench0050["date"].to_list())
print(f"price-only:   CAGR {m_price['cagr']*100:.2f}%  cum {m_price['cum']*100:.1f}%")
print(f"total-return: CAGR {m_total['cagr']*100:.2f}%  cum {m_total['cum']*100:.1f}%\n")

# Per-ETF metrics
print(f"{'ETF':<8}{'name':<26}{'inception':<12}{'days':>5}"
      f"{'cum %':>9}{'CAGR %':>8}{'Sharpe':>8}{'Sortino':>9}{'MDD %':>8}"
      f"{'vol %':>7}{'Beta':>6}{'Alpha %':>9}{'vs 0050 CAGR':>14}")
print("-" * 130)
results = []
for tk in sorted(set(prices['company_code'].to_list()), key=lambda x: (x[0] != '0', x)):
    sub_p = prices.filter(pl.col("company_code") == tk)
    sub_d = divs.filter(pl.col("company_code") == tk)
    if sub_p.height < 5: continue
    name = sub_p["company_name"][0]
    sub = total_return_series(sub_p, sub_d)
    dates = sub["date"].to_list()
    adj = sub["adj_close"].to_numpy()
    m = metrics(adj, dates)
    # bench (0050) over SAME window, adjusted
    b = bench0050.filter((pl.col("date") >= dates[0]) & (pl.col("date") <= dates[-1])).sort("date")
    b_arr = b["adj_close"].to_numpy()
    b_dates = b["date"].to_list()
    if len(b_arr) >= 5:
        bm = metrics(b_arr, b_dates)
        a_rets = np.diff(adj) / adj[:-1]
        m_rets = np.diff(b_arr) / b_arr[:-1]
        n = min(len(a_rets), len(m_rets))
        cov = np.cov(a_rets[:n], m_rets[:n])[0, 1]; var = np.var(m_rets[:n])
        beta = cov / var if var > 0 else 0
        alpha = (a_rets[:n].mean() - beta * m_rets[:n].mean()) * 252
        excess = m['cagr'] - bm['cagr']
    else:
        beta, alpha, excess = 0, 0, 0
    results.append((tk, name, m, beta, alpha, excess))
    print(f"{tk:<8}{name[:24]:<26}{str(dates[0]):<12}{m['days']:>5}"
          f"{m['cum']*100:>8.1f}%{m['cagr']*100:>7.1f}%"
          f"{m['sharpe']:>8.2f}{m['sortino']:>9.2f}{m['mdd']*100:>7.1f}%"
          f"{m['vol']*100:>6.1f}%{beta:>6.2f}{alpha*100:>8.1f}%{excess*100:>13.1f}pp")

# iter_21 same-window
strat = pl.read_csv("research/strat_lab/results/iter_21_daily.csv").with_columns(pl.col("date").str.to_date())
print(f"\n=== 我方 iter_21 對齊各主動 ETF 上市窗口（apples-to-apples，皆 dividend-adjusted）===")
print(f"{'ETF':<8}{'window start':<13}{'days':>5}"
      f"{'iter21 cum':>11}{'ETF cum':>9}"
      f"{'iter21 CAGR':>13}{'ETF CAGR':>11}{'gap':>9}"
      f"{'iter21 Sortino':>16}{'ETF Sortino':>13}")
print("-" * 130)
for tk, name, em, _, _, _ in results:
    if tk in ('0050', '0052'): continue
    sub_p = prices.filter(pl.col("company_code") == tk)
    sub_d = divs.filter(pl.col("company_code") == tk)
    sub = total_return_series(sub_p, sub_d)
    s_d = sub["date"][0]; e_d = sub["date"][-1]
    iter21_sub = strat.filter((pl.col("date") >= s_d) & (pl.col("date") <= e_d))
    if iter21_sub.height < 5: continue
    im = metrics(iter21_sub["nav"].to_numpy(), iter21_sub["date"].to_list())
    print(f"{tk:<8}{str(s_d):<13}{em['days']:>5}"
          f"{im['cum']*100:>10.1f}%{em['cum']*100:>8.1f}%"
          f"{im['cagr']*100:>12.1f}%{em['cagr']*100:>10.1f}%"
          f"{(im['cagr']-em['cagr'])*100:>8.1f}pp"
          f"{im['sortino']:>16.2f}{em['sortino']:>13.2f}")
con.close()
