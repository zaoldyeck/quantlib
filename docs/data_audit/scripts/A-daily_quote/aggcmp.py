import json, subprocess, csv, io, collections
S='/private/tmp/claude-501/-Users-zaoldyeck-Documents-scala-quantlib/3d5413eb-b7db-45c8-bf62-efdef11c1375/scratchpad'
agg=json.load(open(f'{S}/agg.json'))
NUM=['trade_volume','transaction','trade_value','opening_price','highest_price','lowest_price',
     'closing_price','change','last_best_bid_price','last_best_bid_volume','last_best_ask_price',
     'last_best_ask_volume','price_earning_ratio']
sel=",".join([f"count(*) filter (where {c} is null) nl_{c}, coalesce(sum({c}),0) sm_{c}" for c in NUM])
q=f"copy (select market, date::text, count(*) n, {sel} from daily_quote group by 1,2) to stdout csv"
out=subprocess.run(['psql','-h','localhost','-p','5432','-d','quantlib','-Atc',q],capture_output=True,text=True)
assert out.returncode==0, out.stderr
cols=['market','date','n']+sum([[f'nl_{c}',f'sm_{c}'] for c in NUM],[])
db={}
for r in csv.reader(io.StringIO(out.stdout)):
    d=dict(zip(cols,r)); db[(d['market'],d['date'])]=d
bad=collections.Counter(); ex={}
only_raw=[]; only_db=[]
for k,a in agg.items():
    m,dt=k.split('|')
    if (m,dt) not in db: only_raw.append(k); continue
    d=db[(m,dt)]
    if int(d['n'])!=a['n']:
        bad['n']+=1; ex.setdefault('n',(k,a['n'],d['n']))
    for c in NUM:
        nl_db=int(d[f'nl_{c}']); sm_db=float(d[f'sm_{c}'])
        nl_r=a['null'][c]; sm_r=a['sum'][c]
        if nl_db!=nl_r:
            bad['null_'+c]+=1; ex.setdefault('null_'+c,(k,nl_r,nl_db))
        tol=max(1e-6, abs(sm_db)*1e-9)
        if abs(sm_db-sm_r)>tol:
            bad['sum_'+c]+=1; ex.setdefault('sum_'+c,(k,sm_r,sm_db))
for k in db:
    if f"{k[0]}|{k[1]}" not in agg: only_db.append(k)
print("compared date-market pairs:", len(agg))
print("only in raw (no DB rows):", len(only_raw), only_raw[:10])
print("only in DB (no raw rows):", len(only_db), only_db[:10])
print("MISMATCH counters:", dict(bad))
for k,v in ex.items(): print("   ex", k, v)
