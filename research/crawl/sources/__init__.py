"""每個台股資料源一個 adapter:fetch → parse(當前格式)→ cache-schema polars DF。

統一介面(日頻源):`TABLE`、`KEY_COLS`、`MARKETS`、`fetch_day(market, day) -> DF|None`
(None = 交易所回無資料 / 休市)。月頻源(operating_revenue、ex_right_dividend)另有
`fetch_month`。欄位佈局對非預期格式一律 `parse.SchemaDrift` fail-loud。
"""
