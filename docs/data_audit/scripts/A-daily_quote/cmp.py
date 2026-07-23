import sys, os, subprocess, csv, io, datetime, math
sys.path.insert(0,'/private/tmp/claude-501/-Users-zaoldyeck-Documents-scala-quantlib/3d5413eb-b7db-45c8-bf62-efdef11c1375/scratchpad')
import indep

COLS=['company_code','company_name','trade_volume','transaction','trade_value','opening_price','highest_price',
      'lowest_price','closing_price','change','last_best_bid_price','last_best_bid_volume','last_best_ask_price',
      'last_best_ask_volume','price_earning_ratio']

def db_rows(market, d):
    q=(f"copy (select company_code,company_name,trade_volume,transaction,trade_value,opening_price,highest_price,"
       f"lowest_price,closing_price,change,last_best_bid_price,last_best_bid_volume,last_best_ask_price,"
       f"last_best_ask_volume,price_earning_ratio from daily_quote where market='{market}' and date='{d}') to stdout csv")
    out=subprocess.run(['psql','-h','localhost','-p','5432','-d','quantlib','-Atc',q],capture_output=True,text=True)
    if out.returncode!=0: raise RuntimeError(out.stderr)
    res={}
    for r in csv.reader(io.StringIO(out.stdout)):
        rec=dict(zip(COLS,r))
        res[rec['company_code']]=rec
    return res

def norm(v):
    if v is None or v=='': return None
    if isinstance(v,tuple): return v
    try: return float(v)
    except (TypeError,ValueError): return v

def eq(a,b):
    a=norm(a); b=norm(b)
    if a is None and b is None: return True
    if a is None or b is None: return False
    if isinstance(a,float) and isinstance(b,float): return abs(a-b) < 1e-6 or (abs(b)>1 and abs(a-b)/abs(b)<1e-9)
    return str(a)==str(b)

SAMPLES=[('twse','2004-02-11'),('twse','2004-12-30'),('twse','2009-01-05'),('twse','2015-01-05'),
         ('twse','2020-01-02'),('twse','2020-03-19'),('twse','2026-07-17'),('twse','2026-01-02'),
         ('tpex','2007-07-02'),('tpex','2012-01-02'),('tpex','2015-01-05'),('tpex','2020-04-29'),
         ('tpex','2020-04-30'),('tpex','2024-01-02'),('tpex','2026-07-17'),('tpex','2026-01-02')]
grand={}
for market,ds in SAMPLES:
    d=datetime.date.fromisoformat(ds)
    path=f"data/daily_quote/{market}/{d.year}/{d.year}_{d.month}_{d.day}.csv"
    if not os.path.exists(path): print("MISSING FILE", path); continue
    rows = indep.parse_twse(path) if market=='twse' else indep.parse_tpex(path)
    db = db_rows(market, ds)
    mism={}
    only_raw=[r['company_code'] for r in rows if r['company_code'] not in db]
    only_db=[c for c in db if c not in {r['company_code'] for r in rows}]
    for r in rows:
        c=r['company_code']
        if c not in db: continue
        for col in COLS:
            if not eq(r.get(col), db[c][col]):
                mism.setdefault(col,[]).append((c, r.get(col), db[c][col]))
    print(f"== {market} {ds}: raw={len(rows)} db={len(db)} only_raw={len(only_raw)} only_db={len(only_db)}")
    if only_raw[:5]: print("   only_raw ex:", only_raw[:5])
    if only_db[:5]: print("   only_db ex:", only_db[:5])
    for col,v in mism.items():
        print(f"   MISMATCH {col}: {len(v)}  ex={v[:3]}")
        grand[col]=grand.get(col,0)+len(v)
print("GRAND mismatch by column:", grand)
