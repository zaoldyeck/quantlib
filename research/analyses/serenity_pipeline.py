import duckdb
import pandas as pd
import numpy as np
import json
import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from research import db
def run_pipeline():
    con = db.connect()
    
    query = """
    WITH latest_quote AS (
        SELECT company_code, closing_price as price, date as quote_date, trade_volume,
               ROW_NUMBER() OVER(PARTITION BY company_code ORDER BY date DESC) as rn
        FROM daily_quote
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
        m_ly.ni_ttm as ni_ttm_ly
    FROM latest_quote q
    JOIN latest_pe p ON q.company_code = p.company_code AND p.rn = 1
    JOIN latest_rev r ON q.company_code = r.company_code
    JOIN vol_20d v ON q.company_code = v.company_code
    JOIN ranked_margin m ON q.company_code = m.company_code AND m.rn = 1
    LEFT JOIN ranked_margin m_ly ON q.company_code = m_ly.company_code AND m_ly.rn = 5
    WHERE q.rn = 1
      AND r.avg_3m_yoy >= 15 
    """
    
    df = con.sql(query).df()
    if df.empty:
        print(json.dumps({"chunks": []}))
        return

    # Calculate Margin Expansion Factor
    df['margin_expansion_factor'] = 1.0
    df['margin_expansion_factor'] = np.where(df['d_gross_margin_yoy'] > 2, 1.2, df['margin_expansion_factor'])
    df['margin_expansion_factor'] = np.where(df['d_gross_margin_yoy'] > 5, 1.5, df['margin_expansion_factor'])
    
    # Calculate Momentum Acceleration
    df['accel_multiplier'] = 1.0
    cond_accel = (df['yoy_m1'] > df['yoy_m2']) & (df['yoy_m2'] > df['yoy_m3'])
    df['accel_multiplier'] = np.where(cond_accel, 1.3, df['accel_multiplier'])
    
    df['avg_3m_yoy_clamped'] = df['avg_3m_yoy'].clip(lower=1)
    df['true_g'] = df['avg_3m_yoy_clamped'] * df['margin_expansion_factor'] * df['accel_multiplier']
    
    df['pe'] = pd.to_numeric(df['pe'], errors='coerce').fillna(-1)
    
    df['eps'] = np.where(df['pe'] > 0, df['price'] / df['pe'], 0)
    df['forward_eps'] = df['eps'] * (1 + (df['true_g'] / 200))
    df['g_eff'] = 30 * np.log2(1 + df['true_g'] / 30)
    
    df['forward_pe_normal'] = np.where(df['forward_eps'] > 0, df['price'] / df['forward_eps'], -1)
    df['forward_peg'] = np.where(df['forward_pe_normal'] > 0, df['forward_pe_normal'] / df['g_eff'], 999)
    
    df['ni_yoy'] = np.where(
        df['ni_ttm_ly'] > 0,
        ((df['ni_ttm_cur'] - df['ni_ttm_ly']) / df['ni_ttm_ly']) * 100,
        np.where(df['ni_ttm_cur'] > 0, 999, -999)
    )

    use_turnaround = (df['pe'] < 5) | (df['pe'] > 50)
    df['val_engine'] = np.where(use_turnaround, '🚀 營收轉機', '📊 PEG獲利')
    
    # Filter and sort
    peg_df = df[df['val_engine'] == '📊 PEG獲利'].sort_values(by='forward_peg', ascending=True)
    turnaround_df = df[df['val_engine'] == '🚀 營收轉機'].sort_values(by=['ni_yoy', 'true_g'], ascending=[False, False])
    
    top_peg = peg_df.head(20)['company_code'].tolist()
    top_turnaround = turnaround_df.head(20)['company_code'].tolist()
    
    all_targets = list(set(top_peg + top_turnaround))
    
    # Chunk into groups of 10
    chunks = [all_targets[i:i + 10] for i in range(0, len(all_targets), 10)]
    
    output = {
        "total_found": len(all_targets),
        "chunks": chunks
    }
    
    print(json.dumps(output, indent=2))

if __name__ == "__main__":
    run_pipeline()
