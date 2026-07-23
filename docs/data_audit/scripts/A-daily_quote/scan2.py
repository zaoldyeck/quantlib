import glob, os, re, collections, datetime, csv
def keyf(p):
    m = re.search(r'(\d+)_(\d+)_(\d+)\.csv$', p); return datetime.date(int(m.group(1)),int(m.group(2)),int(m.group(3)))
# TWSE: when sign is 'X' or ' ', is 漲跌價差 non-zero?
bad=collections.Counter(); ex={}
for f in sorted(glob.glob('data/daily_quote/twse/*/*.csv'), key=keyf):
    if os.path.getsize(f)==0: continue
    txt=open(f,'rb').read().decode('big5hkscs', errors='replace')
    lines=txt.splitlines()
    try: hi=next(i for i,l in enumerate(lines) if l.startswith('"證券代號"'))
    except StopIteration: continue
    for l in lines[hi+1:]:
        row=next(csv.reader([l.replace('=','')]), None)
        if row is None or len(row)<17: continue
        sign=row[9].strip(); diff=row[10].replace(',','').strip()
        if sign in ('X','',) :
            try: dv=float(diff)
            except ValueError: dv=None
            if dv not in (0.0, None):
                bad[sign]+=1
                ex.setdefault(sign, (f,row[0],row[8],sign,diff))
print("TWSE rows where sign in {X,blank} but 漲跌價差 != 0:", dict(bad))
for k,v in ex.items(): print("  ", repr(k), v)
