"""Independent re-implementation of daily_quote parsing (does NOT call TradingReader).
Columns are located BY HEADER NAME so any positional drift in the Scala reader shows up."""
import csv, datetime, re, os

def _decode(path):
    return open(path,'rb').read().decode('big5hkscs', errors='replace')

def _cells(line):
    # TWSE prefixes text fields with '=' to stop Excel auto-formatting: ="0050"
    return next(csv.reader([line.replace('="','"')]), None)

def _clean(s):
    return s.replace(',','').replace('%','').replace(' ','').strip() if s is not None else s

TWSE_MAP = {  # header name -> canonical field
 '證券代號':'company_code','證券名稱':'company_name','成交股數':'trade_volume','成交筆數':'transaction',
 '成交金額':'trade_value','開盤價':'opening_price','最高價':'highest_price','最低價':'lowest_price',
 '收盤價':'closing_price','漲跌(+/-)':'sign','漲跌價差':'abs_change','最後揭示買價':'last_best_bid_price',
 '最後揭示買量':'last_best_bid_volume','最後揭示賣價':'last_best_ask_price','最後揭示賣量':'last_best_ask_volume',
 '本益比':'price_earning_ratio'}

TPEX_MAP = {
 '代號':'company_code','名稱':'company_name','收盤':'closing_price','漲跌':'signed_change','開盤':'opening_price',
 '最高':'highest_price','最低':'lowest_price','成交股數':'trade_volume','成交金額(元)':'trade_value',
 '成交筆數':'transaction','最後買價':'last_best_bid_price','最後買量(千股)':'last_best_bid_volume',
 '最後買量(張數)':'last_best_bid_volume','最後賣價':'last_best_ask_price','最後賣量(千股)':'last_best_ask_volume',
 '最後賣量(張數)':'last_best_ask_volume','發行股數':'shares_outstanding','次日漲停價':'next_limit_up',
 '次日跌停價':'next_limit_down'}

def _num(v, nulls):
    v = _clean(v)
    if v in nulls: return None
    if v in ('','X'): return 0.0
    if v == '+': return 1.0
    if v == '-': return -1.0
    if v in ('除權息','除權','除息'): return 0.0
    try: return float(v)
    except ValueError: return ('UNPARSEABLE', v)

def parse_twse(path):
    txt=_decode(path); lines=txt.splitlines()
    hi=None
    for i,l in enumerate(lines):
        if l.startswith('"證券代號"'): hi=i; break
    if hi is None: return []
    hdr=[h.strip() for h in _cells(lines[hi])]
    idx={TWSE_MAP[h]:i for i,h in enumerate(hdr) if h in TWSE_MAP}
    assert len(idx)==16, (path, hdr)
    out=[]
    for l in lines[hi+1:]:
        r=_cells(l)
        if r is None or len(r)<17: continue
        g=lambda k: r[idx[k]]
        sign=_clean(g('sign'))
        absch=_num(g('abs_change'), {'--'})
        change = -absch if sign=='-' else absch
        out.append(dict(
            company_code=_clean(g('company_code')), company_name=_clean(g('company_name')),
            trade_volume=_num(g('trade_volume'), {'--'}), transaction=_num(g('transaction'), {'--'}),
            trade_value=_num(g('trade_value'), {'--'}),
            opening_price=_num(g('opening_price'), {'--'}), highest_price=_num(g('highest_price'), {'--'}),
            lowest_price=_num(g('lowest_price'), {'--'}), closing_price=_num(g('closing_price'), {'--'}),
            change=change,
            last_best_bid_price=_num(g('last_best_bid_price'), {'--'}),
            last_best_bid_volume=_num(g('last_best_bid_volume'), {'--'}),
            last_best_ask_price=_num(g('last_best_ask_price'), {'--'}),
            last_best_ask_volume=_num(g('last_best_ask_volume'), {'--'}),
            price_earning_ratio=_num(g('price_earning_ratio'), {'--'}),
            _sign=sign, _raw=r))
    return out

def parse_tpex(path):
    txt=_decode(path); lines=txt.splitlines()
    hi=None
    for i,l in enumerate(lines):
        if l.startswith('代號,'): hi=i; break
    if hi is None: return []
    hdr=[h.strip() for h in lines[hi].split(',')]
    idx={TPEX_MAP[h]:i for i,h in enumerate(hdr) if h in TPEX_MAP}
    NUL={'---','----'}
    out=[]
    for l in lines[hi+1:]:
        r=_cells(l)
        if r is None or len(r)<15: continue
        g=lambda k: r[idx[k]] if k in idx else None
        ch=_num(g('signed_change'), NUL)
        out.append(dict(
            company_code=_clean(g('company_code')), company_name=_clean(g('company_name')),
            trade_volume=_num(g('trade_volume'), NUL), transaction=_num(g('transaction'), NUL),
            trade_value=_num(g('trade_value'), NUL),
            opening_price=_num(g('opening_price'), NUL), highest_price=_num(g('highest_price'), NUL),
            lowest_price=_num(g('lowest_price'), NUL), closing_price=_num(g('closing_price'), NUL),
            change=0.0 if ch is None else ch,
            last_best_bid_price=_num(g('last_best_bid_price'), NUL),
            last_best_bid_volume=(None if 'last_best_bid_volume' not in idx else _num(g('last_best_bid_volume'), NUL)),
            last_best_ask_price=_num(g('last_best_ask_price'), NUL),
            last_best_ask_volume=(None if 'last_best_ask_volume' not in idx else _num(g('last_best_ask_volume'), NUL)),
            price_earning_ratio=None,
            shares_outstanding=_num(g('shares_outstanding'), NUL),
            _raw=r))
    return out
