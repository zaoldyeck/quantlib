import os
import sys
import argparse
import pandas as pd
import numpy as np

sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from research import db
def main():
    parser = argparse.ArgumentParser(description='Serenity Quantitative Valuation Engine V2')
    parser.add_argument('--codes', type=str, help='Comma separated list of stock codes to value (e.g., 2330,3661)', default=None)
    args = parser.parse_args()

    con = db.connect()
    
    # === 1. Macro Regime Filter (大盤年線) ===
    taiex_query = """
        SELECT close 
        FROM market_index 
        WHERE name = '發行量加權股價指數' 
        ORDER BY date DESC 
        LIMIT 200
    """
    taiex_df = con.sql(taiex_query).df()
    macro_warning = False
    taiex_msg = ""
    if len(taiex_df) >= 200:
        latest_close = taiex_df.iloc[0]['close']
        ma200 = taiex_df['close'].mean()
        if latest_close < ma200:
            macro_warning = True
            taiex_msg = f"[🚨 系統紅色警戒] TAIEX ({latest_close:.2f}) 跌破 200 日年線 ({ma200:.2f})！強制暫停所有 ALL-IN 買進動作！\n"
        else:
            taiex_msg = f"[✅ 宏觀綠燈] TAIEX ({latest_close:.2f}) 穩站 200 日年線 ({ma200:.2f}) 之上。\n"
    else:
         taiex_msg = "[⚠️ 警告] 大盤數據不足 200 天，無法計算年線。\n"
         
    print(taiex_msg)
    
    df_max_date = con.sql("SELECT MAX(date) as max_date FROM daily_quote").df()
    max_date = df_max_date.iloc[0]['max_date']
    print(f"INFO: Database latest daily quote date is {max_date}.\n")
    
    code_filter_sql = ""
    if args.codes:
        codes_list = [c.strip() for c in args.codes.split(',')]
        codes_str = ", ".join([f"'{c}'" for c in codes_list])
        code_filter_sql = f"AND q.company_code IN ({codes_str})"
        print(f"🎯 啟動特務估值模式 (Targeting Mode) - 掃描標的: {codes_list}\n")
    else:
        print("🌐 啟動全市場盲掃模式 (Full Market Scan)\n")
    
    # 2. Base Query
    strict_filter_sql = """
      AND r.avg_3m_yoy >= 15 
    """
    
    if args.codes:
        strict_filter_sql = "" 
        
    query = f"""
    WITH quote_data AS (
        SELECT company_code, closing_price as price, date as quote_date, trade_volume,
               AVG(closing_price) OVER(PARTITION BY company_code ORDER BY date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) as ma20,
               AVG(closing_price) OVER(PARTITION BY company_code ORDER BY date ROWS BETWEEN 59 PRECEDING AND CURRENT ROW) as ma60,
               ROW_NUMBER() OVER(PARTITION BY company_code ORDER BY date DESC) as rn
        FROM daily_quote
    ),
    ma_slope AS (
        SELECT company_code, quote_date, ma20,
               LAG(ma20, 5) OVER(PARTITION BY company_code ORDER BY quote_date ASC) as ma20_shift_5
        FROM quote_data
    ),
    latest_quote AS (
        SELECT q.company_code, q.price, q.quote_date, q.trade_volume, q.ma20, q.ma60, s.ma20_shift_5, q.rn
        FROM quote_data q
        JOIN ma_slope s ON q.company_code = s.company_code AND q.quote_date = s.quote_date
        WHERE q.rn = 1
    ),
    vol_20d AS (
        SELECT company_code, AVG(trade_volume) as avg_20d_vol
        FROM (
             SELECT company_code, trade_volume,
                    ROW_NUMBER() OVER(PARTITION BY company_code ORDER BY date DESC) as rn
             FROM daily_quote
        ) WHERE rn <= 20
        GROUP BY company_code
    ),
    latest_pe AS (
        SELECT company_code, price_to_earning_ratio as pe,
               ROW_NUMBER() OVER(PARTITION BY company_code ORDER BY date DESC) as rn
        FROM stock_per_pbr
    ),
    ranked_margin AS (
        SELECT company_code, gross_margin_ttm, d_gross_margin_yoy, cfo_ttm, ni_ttm, year, quarter,
               ROW_NUMBER() OVER(PARTITION BY company_code ORDER BY year DESC, quarter DESC) as rn
        FROM raw_quarterly
    ),
    latest_rev AS (
        SELECT company_code, MAX(company_name) as company_name,
               AVG(monthly_revenue_yoy) as avg_3m_yoy,
               SUM(CASE WHEN rn=1 THEN monthly_revenue_yoy ELSE 0 END) as yoy_m1,
               SUM(CASE WHEN rn=2 THEN monthly_revenue_yoy ELSE 0 END) as yoy_m2,
               SUM(CASE WHEN rn=3 THEN monthly_revenue_yoy ELSE 0 END) as yoy_m3
        FROM (
            SELECT company_code, company_name, monthly_revenue_yoy,
                   ROW_NUMBER() OVER(PARTITION BY company_code ORDER BY year DESC, month DESC) as rn
            FROM operating_revenue
        ) WHERE rn <= 3
        GROUP BY company_code
    )
    SELECT 
        q.company_code,
        r.company_name,
        q.price,
        v.avg_20d_vol,
        p.pe,
        r.avg_3m_yoy,
        r.yoy_m1,
        r.yoy_m2,
        r.yoy_m3,
        m.gross_margin_ttm,
        m.d_gross_margin_yoy,
        m.cfo_ttm,
        m.ni_ttm as ni_ttm_cur,
        m_ly.ni_ttm as ni_ttm_ly,
        q.ma20,
        q.ma60,
        q.ma20_shift_5
    FROM latest_quote q
    JOIN latest_pe p ON q.company_code = p.company_code AND p.rn = 1
    JOIN latest_rev r ON q.company_code = r.company_code
    JOIN vol_20d v ON q.company_code = v.company_code
    JOIN ranked_margin m ON q.company_code = m.company_code AND m.rn = 1
    LEFT JOIN ranked_margin m_ly ON q.company_code = m_ly.company_code AND m_ly.rn = 5
    WHERE q.rn = 1
      {code_filter_sql}
      {strict_filter_sql}
    """
    
    df = con.sql(query).df()
    
    if df.empty:
        print("No stocks passed the valuation engine (or codes not found).")
        return
        
    df['ni_yoy'] = ((df['ni_ttm_cur'] - df['ni_ttm_ly']) / df['ni_ttm_ly'].abs()) * 100
        
    # === 3. Dual Valuation Engine: Margin-Adjusted PEG vs Sales Multiplier ===
    df['d_gross_margin_yoy'] = df['d_gross_margin_yoy'].fillna(0)
    df['margin_expansion_factor'] = 1 + np.maximum(df['d_gross_margin_yoy'], 0)
    
    df['acceleration'] = ((df['yoy_m1'] - df['yoy_m2']) + (df['yoy_m2'] - df['yoy_m3'])) / 2
    df['accel_multiplier'] = 1 + (df['acceleration'] / 100).clip(lower=-0.2, upper=0.2)
    
    df['avg_3m_yoy_clamped'] = df['avg_3m_yoy'].clip(lower=1)
    df['true_g'] = df['avg_3m_yoy_clamped'] * df['margin_expansion_factor'] * df['accel_multiplier']
    
    # Safe PE parsing (handle negative or zero PE)
    df['pe'] = pd.to_numeric(df['pe'], errors='coerce').fillna(-1)
    
    # === MA Slope Penalty ===
    # Check if MA20 is sloping downwards (current ma20 < ma20 5 days ago)
    df['ma20_bending_down'] = df['ma20'] < df['ma20_shift_5']
    
    # Base target PE multiplier logic
    # Default 1.2. If price < 20MA and bending down, 0.8. If price < 60MA, 0.4.
    df['trend_multiplier'] = np.where(
        df['price'] < df['ma60'], 0.4,
        np.where((df['price'] < df['ma20']) & df['ma20_bending_down'], 0.8, 1.2)
    )

    # 1. Normal Track (PEG Model)
    df['eps'] = np.where(df['pe'] > 0, df['price'] / df['pe'], 0)
    df['forward_eps'] = df['eps'] * (1 + (df['true_g'] / 200))
    df['g_eff'] = 30 * np.log2(1 + df['true_g'] / 30)
    
    df['forward_pe_normal'] = np.where(df['forward_eps'] > 0, df['price'] / df['forward_eps'], -1)
    df['forward_peg_normal'] = np.where(df['forward_pe_normal'] > 0, df['forward_pe_normal'] / df['g_eff'], 999)
    df['target_pe'] = df['trend_multiplier'] * df['g_eff']
    df['target_price_normal'] = df['forward_eps'] * df['target_pe']
    
    # 2. Turnaround Track (Revenue Sales Multiplier Model)
    # Target Price = Current Price * (1 + min(True Growth / 100 * 0.8, 1.0))
    # Cap upside to 100% to avoid fake signals from low-base explosive YoY
    df['turnaround_upside'] = np.minimum((df['true_g'] / 100) * 0.8, 1.0)
    df['target_price_turnaround'] = df['price'] * (1 + df['turnaround_upside'])
    
    # 3. Dual Engine Switch Logic
    use_turnaround = (df['pe'] < 5) | (df['pe'] > 50)
    
    df['target_price'] = np.where(use_turnaround, df['target_price_turnaround'], df['target_price_normal'])
    df['forward_pe'] = np.where(use_turnaround, -1, df['forward_pe_normal'])
    df['forward_peg'] = np.where(use_turnaround, 1.0, df['forward_peg_normal'])
    
    df['val_engine'] = np.where(use_turnaround, '🚀 營收轉機', '📊 PEG獲利')
    
    # === 4. Liquidity & Stop Loss Sizing ===
    valid_codes = tuple(df['company_code'].tolist())
    if valid_codes:
        if len(valid_codes) == 1:
            sql_tuple = f"('{valid_codes[0]}')"
        else:
            sql_tuple = str(valid_codes)
            
        vol_query = f"""
        SELECT company_code, date, opening_price, highest_price, lowest_price, closing_price
        FROM daily_quote
        WHERE company_code IN {sql_tuple}
        ORDER BY company_code, date DESC
        """
        hist_df = con.sql(vol_query).df()
        
        vol_dict = {}
        atr_stop_dict = {}
        for code in valid_codes:
            code_hist = hist_df[hist_df['company_code'] == code].head(22).copy()
            if len(code_hist) > 1:
                code_hist = code_hist.sort_values('date').reset_index(drop=True)
                returns = code_hist['closing_price'].pct_change(fill_method=None).dropna()
                annual_vol = returns.std() * np.sqrt(252)
                vol_dict[code] = annual_vol
                
                code_hist['prev_close'] = code_hist['closing_price'].shift(1)
                code_hist['tr1'] = code_hist['highest_price'] - code_hist['lowest_price']
                code_hist['tr2'] = abs(code_hist['highest_price'] - code_hist['prev_close'])
                code_hist['tr3'] = abs(code_hist['lowest_price'] - code_hist['prev_close'])
                code_hist['tr'] = code_hist[['tr1', 'tr2', 'tr3']].max(axis=1)
                atr20 = code_hist['tr'].tail(20).mean()
                
                current_price = code_hist['closing_price'].iloc[-1]
                atr_stop = current_price - (2.5 * atr20)
                
                atr_stop_dist_pct = (current_price - atr_stop) / current_price
                if atr_stop_dist_pct < 0.10:
                    atr_stop = current_price * 0.90
                elif atr_stop_dist_pct > 0.25:
                    atr_stop = current_price * 0.75
                atr_stop_dict[code] = atr_stop
            else:
                vol_dict[code] = 0.5 
                atr_stop_dict[code] = df[df['company_code'] == code]['price'].values[0] * 0.85
                
        df['annual_volatility'] = df['company_code'].map(vol_dict)
        df['atr_stop'] = df['company_code'].map(atr_stop_dict)
        target_risk = 0.02 
        df['suggested_position'] = (target_risk / df['annual_volatility']).clip(upper=0.10)
    else:
        df['suggested_position'] = 0.0
        df['annual_volatility'] = 0.0
        df['atr_stop'] = df['price'] * 0.85
        
    df['hard_stop_15pct'] = df['price'] * 0.85
    
    # === 5. Formatting ===
    df = df.sort_values('forward_peg', ascending=True).head(100)
    
    df['accel_trend'] = np.where(df['acceleration'] > 0, "🔥 加速", "📉 減速")
    df['cash_bleed_fmt'] = np.where((df['cfo_ttm'] < 0) & (df['ni_ttm_cur'] < 0), "💀 致命雙殺", np.where(df['cfo_ttm'] < 0, "⚠️ CFO流失", "✅ 健康"))
    df['ni_yoy_fmt'] = df['ni_yoy'].round(1).astype(str) + '%'
    df['ni_yoy_fmt'] = np.where(df['ni_yoy'] <= 0, "❌ " + df['ni_yoy_fmt'], "✅ " + df['ni_yoy_fmt'])
    
    df['liq_warning'] = np.where(df['avg_20d_vol'] < 1000000, "⚠️ 枯竭", "✅ 正常")
    
    df['avg_3m_yoy_pct'] = df['avg_3m_yoy'].round(2).astype(str) + '%'
    df['forward_peg'] = df['forward_peg'].round(3)
    df['forward_pe'] = df['forward_pe'].round(2)
    df['pe'] = df['pe'].round(2)
    df['price'] = df['price'].round(2)
    
    # Apply Hard Stop formatting for broken trends
    df['target_price_numeric'] = pd.to_numeric(df['target_price'], errors='coerce')
    df['target_price'] = df['target_price'].round(2).astype(str)
    
    # If price < 60MA, it's a structural break. Force escape.
    mask_60 = (df['price'] < df['ma60']) & (df['target_price_numeric'] > df['price'])
    df['target_price'] = np.where(mask_60, '💀 季線下彎(逃命)', df['target_price'])
    
    # If price < 20MA and 20MA is bending down, momentum is broken. Force no buy.
    mask_20 = (~mask_60) & (df['price'] < df['ma20']) & df['ma20_bending_down'] & (df['target_price_numeric'] > df['price'])
    df['target_price'] = np.where(mask_20, '📉 趨勢下彎(禁買)', df['target_price'])
    
    df['hard_stop_15pct'] = df['hard_stop_15pct'].round(2)
    
    output_cols = [
        'company_code', 'company_name', 'price', 'hard_stop_15pct', 'target_price', 
        'val_engine', 'pe', 'forward_pe', 'cash_bleed_fmt', 'avg_3m_yoy_pct', 'ni_yoy_fmt', 'accel_trend'
    ]
    
    md_table = df[output_cols].rename(columns={
        'company_code': '代號',
        'company_name': '名稱',
        'price': '現價',
        'hard_stop_15pct': '15%停損',
        'target_price': '目標價',
        'val_engine': '估值引擎',
        'pe': 'TTM_PE',
        'forward_pe': 'Fwd_PE',
        'cash_bleed_fmt': '現金流防線',
        'avg_3m_yoy_pct': '營收 YoY',
        'ni_yoy_fmt': '淨利 YoY (質檢)',
        'accel_trend': '動能'
    }).to_markdown(index=False)
    
    print(md_table)

if __name__ == "__main__":
    main()
