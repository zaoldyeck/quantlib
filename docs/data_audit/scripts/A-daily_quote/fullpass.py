"""Full-history independent re-parse of every daily_quote raw file:
   (a) per-date aggregate fingerprint per column  (b) filename-date vs content-date
   (c) census of blank / sentinel cells that the Scala reader coerces to 0."""
import sys, os, glob, re, json, datetime, collections, csv
sys.path.insert(0,'/private/tmp/claude-501/-Users-zaoldyeck-Documents-scala-quantlib/3d5413eb-b7db-45c8-bf62-efdef11c1375/scratchpad')
import indep

NUMCOLS=['trade_volume','transaction','trade_value','opening_price','highest_price','lowest_price',
         'closing_price','change','last_best_bid_price','last_best_bid_volume','last_best_ask_price',
         'last_best_ask_volume','price_earning_ratio']

def keyf(p):
    m=re.search(r'(\d+)_(\d+)_(\d+)\.csv$',p); return datetime.date(int(m.group(1)),int(m.group(2)),int(m.group(3)))

agg={}      # (market,date) -> {n, sums, nulls}
datemis=[]  # filename vs content date mismatches
blank=collections.Counter()
unparse=collections.Counter(); unparse_ex={}
namecomma=collections.Counter()

for market in ('twse','tpex'):
    for f in sorted(glob.glob(f'data/daily_quote/{market}/*/*.csv'), key=keyf):
        if os.path.getsize(f)==0: continue
        d=keyf(f)
        txt=indep._decode(f)
        # content date
        if market=='twse':
            m=re.search(r'(\d{2,3})年(\d{2})月(\d{2})日', txt)
            cd = datetime.date(int(m.group(1))+1911,int(m.group(2)),int(m.group(3))) if m else None
        else:
            m=re.search(r'資料日期[:：]\s*(\d{2,3})/(\d{2})/(\d{2})', txt)
            cd = datetime.date(int(m.group(1))+1911,int(m.group(2)),int(m.group(3))) if m else None
        if cd is None or cd!=d: datemis.append((market,str(d),str(cd)))
        rows = indep.parse_twse(f) if market=='twse' else indep.parse_tpex(f)
        if not rows: continue
        a={'n':0,'sum':{c:0.0 for c in NUMCOLS},'null':{c:0 for c in NUMCOLS}}
        for r in rows:
            a['n']+=1
            if ',' in (r['_raw'][1] or ''): namecomma[market]+=1
            for c in NUMCOLS:
                v=r.get(c)
                if isinstance(v,tuple):
                    unparse[(market,c)]+=1; unparse_ex.setdefault((market,c),(f,r['company_code'],v)); v=None
                if v is None: a['null'][c]+=1
                else: a['sum'][c]+=v
            # blank census on raw cells
        agg[(market,str(d))]=a
json.dump({f"{k[0]}|{k[1]}":v for k,v in agg.items()}, open('/private/tmp/claude-501/-Users-zaoldyeck-Documents-scala-quantlib/3d5413eb-b7db-45c8-bf62-efdef11c1375/scratchpad/agg.json','w'))
print("dates aggregated:", len(agg))
print("filename-vs-content date mismatches:", len(datemis), datemis[:10])
print("UNPARSEABLE cells:", dict(unparse))
for k,v in list(unparse_ex.items())[:10]: print("   ", k, v)
print("company names containing comma in raw:", dict(namecomma))
