import glob, os, re, collections, datetime, csv, io
def keyf(p):
    m = re.search(r'(\d+)_(\d+)_(\d+)\.csv$', p); return datetime.date(int(m.group(1)),int(m.group(2)),int(m.group(3)))

# 1) TWSE 漲跌(+/-) distinct raw values + lines containing "" not preceded by comma
sign_vals = collections.Counter(); sign_ex = {}
dq_lines = collections.Counter(); dq_ex=[]
for f in sorted(glob.glob('data/daily_quote/twse/*/*.csv'), key=keyf):
    if os.path.getsize(f)==0: continue
    txt=open(f,'rb').read().decode('big5hkscs', errors='replace')
    lines=txt.splitlines()
    try: hi=next(i for i,l in enumerate(lines) if l.startswith('"證券代號"'))
    except StopIteration: continue
    for l in lines[hi+1:]:
        if '""' in l and ',""' not in l:
            dq_lines[os.path.basename(f)]+=1
            if len(dq_ex)<5: dq_ex.append((f,l[:160]))
        row=next(csv.reader([l.replace('=','')]), None)
        if row is None or len(row)<17: continue
        v=row[9]
        sign_vals[v]+=1
        sign_ex.setdefault(v, (f,row[0],row[8],row[10]))
print("TWSE 漲跌(+/-) distinct:", dict(sign_vals))
for k,v in sign_ex.items(): print("   ex", repr(k), v)
print("TWSE lines with unescaped-pair-quote dropped by QuantlibCSVReader:", sum(dq_lines.values()), dict(list(dq_lines.items())[:10]))
for e in dq_ex: print("   ", e)
