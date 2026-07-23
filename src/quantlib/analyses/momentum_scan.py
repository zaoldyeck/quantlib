import os
import sys
import argparse
import pandas as pd

sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from quantlib import db
def main():
    parser = argparse.ArgumentParser(description='Serenity Momentum Scanner')
    parser.add_argument('--top', type=int, default=20, help='Number of top momentum stocks to return')
    args = parser.parse_args()

    con = db.connect()
    
    # Get the latest date
    df_max = con.sql("SELECT MAX(date) as max_date FROM daily_quote").df()
    if df_max.empty or pd.isnull(df_max.iloc[0]['max_date']):
        print("No daily quote data found.")
        sys.exit(1)
        
    max_date = df_max.iloc[0]['max_date']
    
    # Query the last 15 days of data to ensure we get at least 5 trading days
    # (accounting for weekends and holidays)
    query = f"""
        SELECT date, company_code, closing_price, trade_volume
        FROM daily_quote
        WHERE date >= (DATE '{max_date}' - INTERVAL 15 DAY)
    """
    df = con.sql(query).df()
    
    if df.empty:
        print("No recent data found.")
        sys.exit(1)
        
    # Sort by date
    df = df.sort_values(['company_code', 'date'])
    
    # Keep only the last 6 trading days per company (1 base + 5 days of returns)
    df = df.groupby('company_code').tail(6)
    
    # Calculate return
    results = []
    for code, group in df.groupby('company_code'):
        if len(group) >= 2: # At least 2 days to calculate a return
            first_price = group['closing_price'].iloc[0]
            last_price = group['closing_price'].iloc[-1]
            
            # User instruction: "無下限" (No lower limit for liquidity). 
            # We still ensure price > 0 to avoid division by zero.
            if first_price > 0:
                ret = (last_price / first_price) - 1
                results.append({
                    'company_code': code,
                    'momentum_return': ret
                })
                
    res_df = pd.DataFrame(results)
    if res_df.empty:
        print("No valid returns calculated.")
        sys.exit(1)
        
    # Rank by momentum return descending
    res_df = res_df.sort_values('momentum_return', ascending=False).reset_index(drop=True)
    
    top_n = res_df.head(args.top)
    
    print(f"=== TOP {args.top} MOMENTUM STOCKS (Ranked) ===")
    for idx, row in top_n.iterrows():
        print(f"{int(idx)+1}. {row['company_code']} : {row['momentum_return']*100:.2f}%")
        
    # Output raw codes string for easy parsing by subagent
    codes_list = top_n['company_code'].tolist()
    print("\nRAW_CODES:" + ",".join(codes_list))

if __name__ == "__main__":
    main()
