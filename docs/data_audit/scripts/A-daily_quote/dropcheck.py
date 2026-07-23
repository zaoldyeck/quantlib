import sys, glob, os, re, csv, datetime, collections
sys.path.insert(0,'/private/tmp/claude-501/-Users-zaoldyeck-Documents-scala-quantlib/3d5413eb-b7db-45c8-bf62-efdef11c1375/scratchpad')
import indep
def keyf(p):
    m=re.search(r'(\d+)_(\d+)_(\d+)\.csv$',p); return datetime.date(int(m.group(1)),int(m.group(2)),int(m.group(3)))
drop=collections.Counter(); ex=collections.defaultdict(list)
for market,hdr_pref,minc in (('twse','"證券代號"',17),('tpex','代號,',15)):
    for f in sorted(glob.glob(f'data/daily_quote/{market}/*/*.csv'), key=keyf):
        if os.path.getsize(f)==0: continue
        lines=indep._decode(f).splitlines()
        hi=next((i for i,l in enumerate(lines) if l.startswith(hdr_pref)), None)
        if hi is None: continue
        for l in lines[hi+1:]:
            r=indep._cells(l)
            if not r: continue
            # looks like a data row? first cell is a stock-code-shaped token
            c0=(r[0] or '').strip()
            if re.fullmatch(r'[0-9]{4,6}[A-Z]?', c0) and len(r)<minc:
                drop[market]+=1
                if len(ex[market])<5: ex[market].append((f, len(r), l[:150]))
print("data-shaped rows dropped by the size filter:", dict(drop))
for k,v in ex.items():
    for e in v: print("  ", k, e)
