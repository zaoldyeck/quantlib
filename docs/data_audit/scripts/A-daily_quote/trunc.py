import sys, glob, os, re, datetime
sys.path.insert(0,'/private/tmp/claude-501/-Users-zaoldyeck-Documents-scala-quantlib/3d5413eb-b7db-45c8-bf62-efdef11c1375/scratchpad')
import indep
def keyf(p):
    m=re.search(r'(\d+)_(\d+)_(\d+)\.csv$',p); return datetime.date(int(m.group(1)),int(m.group(2)),int(m.group(3)))
bad_tpex=[]; bad_twse=[]
for f in sorted(glob.glob('data/daily_quote/tpex/*/*.csv'), key=keyf):
    if os.path.getsize(f)==0: continue
    t=indep._decode(f)
    if not re.search(r'共\d+筆', t): bad_tpex.append((f, os.path.getsize(f)))
for f in sorted(glob.glob('data/daily_quote/twse/*/*.csv'), key=keyf):
    if os.path.getsize(f)==0: continue
    t=indep._decode(f)
    if '"證券代號"' not in t: bad_twse.append((f,'no header',os.path.getsize(f))); continue
    if '備註' not in t.split('"證券代號"',1)[1]: bad_twse.append((f,'no 備註 footer',os.path.getsize(f)))
print("TPEx files missing 共N筆 footer (truncated download):", len(bad_tpex))
for x in bad_tpex[:20]: print("   ", x)
print("TWSE files missing 備註 footer after stock table:", len(bad_twse))
for x in bad_twse[:20]: print("   ", x)
