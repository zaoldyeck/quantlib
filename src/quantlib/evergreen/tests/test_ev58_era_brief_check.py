"""era_brief 出廠檢查的守護:網址樣板不得被當成壞掉的出處。

事故(2026-08-06,E3 重跑):`media_landscape.archive_hint` 的存在意義就是告訴下游
「這站的網址長什麼樣」,所以卡片裡必然有 `https://technews.tw/YYYY/MM/DD/` 這種
樣板。舊版檢查器把全文抓到的每個 http 字串都 curl 一遍,7 條 FAIL 全部來自樣板、
零條是真的壞掉的出處——檢查器自己在製造假警報,而假警報會讓人學會忽略紅燈。
"""

from __future__ import annotations

from quantlib.evergreen.ev58_era_brief_check import split_urls


def test_placeholder_urls_are_not_treated_as_citations() -> None:
    raw = (
        '{"archive_hint": "日期封存頁 https://technews.tw/YYYY/MM/DD/ 會列出當天全部文章;'
        '報紙稿 https://www.chinatimes.com/newspapers/YYYYMMDD00NNNN-2602NN ;'
        '列印版 https://m.cnyes.com/news/print/NNNNNN",'
        '"evidence": "https://www.chinatimes.com/newspapers/20140105000095-260202"}'
    )
    cited, templates = split_urls(raw)
    assert cited == ["https://www.chinatimes.com/newspapers/20140105000095-260202"]
    assert len(templates) == 3


def test_angle_bracket_and_query_placeholders_are_templates() -> None:
    # URL_RE 在 `<` 前停住,留下看似正常、實際被截斷的網址;以及 `?q=` 這種空查詢。
    raw = (
        "電子報封存 https://paper.udn.com/udnpaper/<刊號>/<期數>/web/ 仍在;"
        "看板檢索 https://www.ptt.cc/bbs/Stock/search?q=<關鍵字>"
    )
    cited, templates = split_urls(raw)
    assert cited == []
    assert set(templates) == {
        "https://paper.udn.com/udnpaper/",
        "https://www.ptt.cc/bbs/Stock/search?q=",
    }


def test_wayback_wildcard_query_is_a_template() -> None:
    # 時光機的萬用字元查詢是「去哪裡找」,不是「這一篇」;curl 它必然逾時。
    raw = "只能走 Wayback Machine(https://web.archive.org/web/2014*/tw.appledaily.com/* )。"
    cited, templates = split_urls(raw)
    assert cited == []
    assert templates == ["https://web.archive.org/web/2014*/tw.appledaily.com/*"]


def test_real_urls_with_digits_survive() -> None:
    # 反向守護:真出處常含長數字與百分比編碼,不可被誤判成樣板。
    raw = (
        "https://www.businesstoday.com.tw/article/category/183008/post/201407030011/ "
        "https://ec.ltn.com.tw/article/breakingnews/1334241 "
        "https://www.moneyweekly.com.tw/ArticleData/Info/%E7%90%86%E8%B2%A1%E5%91%A8%E5%88%8A/24477"
    )
    cited, templates = split_urls(raw)
    assert len(cited) == 3
    assert templates == []
