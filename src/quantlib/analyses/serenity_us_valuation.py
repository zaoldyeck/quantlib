import argparse
import yfinance as yf
import pandas as pd

def get_financials(ticker_symbol):
    try:
        ticker = yf.Ticker(ticker_symbol)
        info = ticker.info
        
        # Get basic info
        name = info.get('shortName', ticker_symbol)
        price = info.get('currentPrice', info.get('regularMarketPrice', 0))
        
        # Get P/E
        pe = info.get('forwardPE', info.get('trailingPE', 0))
        
        # Get Margins and Growth
        gross_margin = info.get('grossMargins', 0)
        if gross_margin is None: gross_margin = 0
        rev_growth = info.get('revenueGrowth', 0)
        if rev_growth is None: rev_growth = 0
        earnings_growth = info.get('earningsGrowth', 0)
        if earnings_growth is None: earnings_growth = 0
        
        # Earnings Quality Gate
        if earnings_growth > 0:
            ni_check = f"✅ +{earnings_growth*100:.1f}%"
        else:
            ni_check = f"❌ {earnings_growth*100:.1f}%"
            
        # Adj PEG Calculation (Simplified Serenity Formula)
        if rev_growth > 0 and pe > 0:
            adj_peg = pe / (rev_growth * 100)
        else:
            adj_peg = 999.0
            
        # Get Moving Averages
        ma50 = info.get('fiftyDayAverage', 0)
        if ma50 is None: ma50 = 0
        ma200 = info.get('twoHundredDayAverage', 0)
        if ma200 is None: ma200 = 0
            
        # Determine momentum / decision
        if (ma50 > 0 and price < ma50) or (ma200 > 0 and price < ma200):
            decision = "💀 空頭陷阱 (Falling Knife)"
        elif adj_peg > 1.2 or pe == 0 or rev_growth <= 0:
            decision = "💀 估值泡沫 (Priced-in)"
        elif earnings_growth <= 0:
            decision = "💀 獲利衰退 (Value Trap)"
        else:
            decision = "👑 買進 (Engine 2 ALL-IN)"
            
        return {
            "代號": ticker_symbol,
            "名稱": str(name)[:15],
            "現價": price,
            "PE": round(pe, 2) if pe else "N/A",
            "毛利率": f"{gross_margin*100:.2f}%",
            "營收 YoY": f"{rev_growth*100:.2f}%",
            "淨利 YoY (質檢)": ni_check,
            "Adj_PEG": round(adj_peg, 2) if adj_peg != 999.0 else "N/A",
            "決策判定": decision
        }
    except Exception as e:
        return {
            "代號": ticker_symbol,
            "名稱": "Error",
            "現價": 0,
            "PE": 0,
            "毛利率": "0%",
            "營收 YoY": "0%",
            "淨利 YoY (質檢)": "Error",
            "Adj_PEG": 0,
            "決策判定": "Error"
        }

def main():
    parser = argparse.ArgumentParser(description="Serenity US Valuation Engine")
    parser.add_argument("--codes", type=str, required=True, help="Comma separated list of US tickers")
    args = parser.parse_args()
    
    tickers = [t.strip().upper() for t in args.codes.split(',')]
    print(f"\n🎯 啟動特務估值模式 (US Targeting Mode) - 掃描標的: {tickers}\n")
    
    results = []
    for t in tickers:
        data = get_financials(t)
        results.append(data)
        
    df = pd.DataFrame(results)
    
    # Sort by Adj_PEG
    df['Adj_PEG_sort'] = pd.to_numeric(df['Adj_PEG'], errors='coerce').fillna(999)
    df = df.sort_values(by=['決策判定', 'Adj_PEG_sort']).drop(columns=['Adj_PEG_sort'])
    
    print(df.to_markdown(index=False))

if __name__ == "__main__":
    main()
