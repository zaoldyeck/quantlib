import glob, os, re, collections, datetime, csv
def keyf(p):
    m = re.search(r'(\d+)_(\d+)_(\d+)\.csv$', p); return datetime.date(int(m.group(1)),int(m.group(2)),int(m.group(3)))
total=0; percount={}; footer_mismatch=[]; dq=0
for f in sorted(glob.glob('data/daily_quote/tpex/*/*.csv'), key=keyf):
    if os.path.getsize(f)==0: continue
    txt=open(f,'rb').read().decode('big5hkscs', errors='replace')
    lines=txt.splitlines()
    try: hi=next(i for i,l in enumerate(lines) if l.startswith('代號,'))
    except StopIteration: continue
    ncol=len(lines[hi].split(','))
    n=0
    for l in lines[hi+1:]:
        if '""' in l and ',""' not in l: dq+=1
        row=next(csv.reader([l.replace('=','')]), None)
        if row is None or len(row)<15: continue
        n+=1
    if n: percount[keyf(f)]=n
    total+=n
    # footer 共N筆
    m=[re.search(r'共(\d+)筆', l) for l in lines]
    m=[x for x in m if x]
    if m:
        declared=int(m[-1].group(1))
        if declared!=n: footer_mismatch.append((os.path.basename(f), declared, n))
print("TPEx independent data rows total:", total, "dates with data:", len(percount))
print("lines dropped by quote rule:", dq)
print("footer 共N筆 mismatches:", len(footer_mismatch), footer_mismatch[:10])
import json
json.dump({str(k):v for k,v in percount.items()}, open('/private/tmp/claude-501/-Users-zaoldyeck-Documents-scala-quantlib/3d5413eb-b7db-45c8-bf62-efdef11c1375/scratchpad/tpex_counts.json','w'))
