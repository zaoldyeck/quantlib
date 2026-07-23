import sys, glob, os, re, csv, datetime, collections, random
sys.path.insert(0,'/private/tmp/claude-501/-Users-zaoldyeck-Documents-scala-quantlib/3d5413eb-b7db-45c8-bf62-efdef11c1375/scratchpad')
import indep
def keyf(p):
    m=re.search(r'(\d+)_(\d+)_(\d+)\.csv$',p); return datetime.date(int(m.group(1)),int(m.group(2)),int(m.group(3)))
random.seed(7)
hits=collections.Counter(); ex=collections.defaultdict(set); blank=collections.Counter()
for market,pref,minc in (('twse','"證券代號"',17),('tpex','代號,',15)):
    fs=[f for f in glob.glob(f'data/daily_quote/{market}/*/*.csv') if os.path.getsize(f)>0]
    fs=sorted(random.sample(fs, 120), key=keyf)
    for f in fs:
        lines=indep._decode(f).splitlines()
        hi=next((i for i,l in enumerate(lines) if l.startswith(pref)), None)
        if hi is None: continue
        if any(l.strip()=='' for l in lines[:hi]): blank[market+'_before_hdr']+=1
        for l in lines[hi+1:]:
            r=indep._cells(l)
            if not r or len(r)<minc: continue
            nm=r[1]
            if nm!=nm.strip() or ' ' in nm.strip() or '　' in nm:
                hits[market]+=1
                if len(ex[market])<8: ex[market].add((r[0], repr(nm)))
print("files sampled per market: 120")
print("raw company names with internal/edge spaces:", dict(hits))
for k,v in ex.items(): print("  ", k, list(v)[:8])
print("blank lines before header:", dict(blank))
