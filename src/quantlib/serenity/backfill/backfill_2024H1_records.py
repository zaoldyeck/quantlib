"""2024H1 回溯標記 — 種子聚類檢核記錄補全(判準 B 完備性).

背景:2024H1 批(2024-01~06)的標記 agent 把多個「既檢核產業沿用」的種子聚類
折進各月單一合併群記錄(industry 欄寫成「半導體/電子零組件/通信網路/…(既檢核
沿用)」,省略「業」字並以斜線併列),導致 `pilot_acceptance.py` 判準 B 的子字串
比對(`seed industry in cluster industry`,如 `"半導體業" in "半導體/電子零組件…"`)
落空,共 80 項缺檢。檢核其實有做(合併群 narrative 逐一交代了各產業判定),只是
未按 v4 條款「每個種子聚類各寫一行、industry 欄含原始產業名」落地。真正的新群
(電子通路業 momentum、建材營造、文化創意業、航運業、電機機械工具機)都已有各自
記錄,不在缺檢清單。

本腳本把 80 項補回各月 `label_runs/{month}.json` 的 clusters 陣列:

- **carry_over(79 項)**:該種子產業與成員構成和先前已檢核者實質相同(承 2022H2+
  2023 既有判定),沿用既有 reject/defer/beneficiary 裁決。empty evidence → 判準 C
  零違規;verdict=carry_over → 不進判準 A 的事後報酬表。narrative 逐項具名成員並
  交叉引用在冊真瓶頸(CoWoS `advanced_packaging_cowos` / 重電 `grid_heavy_electrical`
  / 液冷觀察線 `ai_liquid_cooling`),誠實標明 beneficiary/defer/structural-reject 之別。

- **完整重檢(1 項)**:2024-01 revenue 半導體業 —— 含記憶體模組廠十銓 4967
  (yoy_3m +195%),是 rule 3 點名的「記憶體早週期〔原廠減產驅動,注意大宗商品
  循環判例〕」細分,亦是 2023H2 記憶體重檢(2023-11 momentum / 2023-12 revenue)
  的直接延續。本次以時間圍欄真搜尋(WebSearch 2026-07,只採發布日 ≤ 2024-01-31
  來源)重檢:≤圍欄敘事為 2023 下半原廠(三星/SK/美光/WD/鎧俠)減產去化、估 2024
  記憶體價落底回升(ctee 2023-08-31 「明年迎春燕」;MoneyDJ 研調「減產奏效 Q1
  跌幅收斂」),屬**典型大宗商品循環**(供給緊縮係原廠可逆的產能調節,非 ≥18 個月
  結構性無彈性),十銓為向三星/SK/美光寡占買晶片、組模組的下游廠——不擁稀缺節點、
  毛利改善屬庫存評價一次性(賺庫存財)、客戶可無痛換模組。瓶頸簽名第 2、第 3 皆
  不通過,凍結提示詞明列「純大宗商品循環」為直接 reject 例 → **reject**。此為
  紀律性裁決:2024-01 當下 reject,與記憶體 2025-2026 超級循環(暫停報價八年首見、
  營收翻倍、DRAM 半年 +340%)無關(時間圍欄,全部丟棄)。同一裁決於 2024-02/03/05/06
  revenue 半導體業以 carry_over 沿用(十銓 4967 續在榜,威剛 3260 於 3 月起加入,
  同屬模組組裝);2024-04 revenue 半導體業無記憶體模組名,為純 IC 設計/ASIC fabless
  受惠端 carry reject。

丟棄的晚於圍欄來源(2024 下半~2026 回溯文,不得作 2024-01 判斷依據,約 20+ 筆):
威剛/十銓暫停報價八年首見(money.udn 9055840,story id 2025-2026)、威剛 2026-01
營收 84 億 +199%/十銓 +112%(nextapple 20260207)、DRAM 半年飆 340%(ctee 20260503)、
64GB RDIMM 450→900 美元(2026)、Q3 合約價 +20%/NAND +35-40%(ctee 20260705)、
SK Hynix 取消長約價格上限、記憶體模組雙雄獲利爆發(money.udn 9470322)等;cnyes
5553838「威剛低價庫存優勢」story id 比對約 2024 年中(> 圍欄),日期不確定,保守丟棄。

GB200 液冷/機構件(rule 3 第二點)已在各月 free_discovery「散熱(AI 液冷)」記錄
完整檢核(2024-01/02 reject 過早 → 2024-03 GTC 後升 defer,子節點富世達 6805 UQD
列 enabler_candidate 觀察未入冊),本補全對電子零組件業以 carry_over 交叉引用之:
富世達 6805(UQD 快接頭,量產 2025 Q2、液冷滲透率 2024 僅 11%,對 H1 圍欄屬前視)、
川湖 2059(伺服器滑軌龍頭 ~50% 市佔,機構受惠端),皆不新增入冊。

依賴:不需 cache_tables.py(只改 JSON);判準驗收 `pilot_acceptance.py` 需 cache。
Run: uv run --project . python -m quantlib.serenity.backfill.backfill_2024H1_records
冪等:以 (industry, seed_type) 去重(限 backfill 標記行),重跑不重複追加。
完成後跑:uv run --project . python -m quantlib.serenity.backfill.pilot_acceptance \
          --months 2024-01 2024-02 2024-03 2024-04 2024-05 2024-06 --tag 2024H1
"""

from __future__ import annotations

import json
from pathlib import Path

HERE = Path(__file__).parent
LR = HERE / "label_runs"
BACKFILL = "2024H1_recordkeeping"

# ---- 既檢核產業沿用敘事(成員名錨定各月已讀種子;承 2022H2+2023 判定)----
SEMI_ICDESIGN = (
    "沿用既有判定:半導體業動能群為 IC 設計 fabless / 利基受惠端(安國 8054 / 金麗科 3228 / "
    "芯鼎 6695 / 揚智 3041 / 世紀 5314 / 祥碩 5269 / 世芯-KY 3661 / 創意 3443 相鄰等),riding "
    "AI/HPC 晶片需求但不擁實體產能瓶頸節點,carry reject(fabless 受惠)。此鏈真瓶頸為先進封裝 "
    "CoWoS,已在冊(advanced_packaging_cowos,台積電 2330 owner=5)。成員逐月輪動但同屬 IC 設計/"
    "利基類別,無新供應鏈敘事。"
)
SEMI_REV_MEM_REASON = (
    "記憶體模組廠十銓 4967(TWSE 上市)於 2024-01 半導體營收加速榜(近三月 YoY +195%,前期 +148%):"
    "以時間圍欄真搜尋(只採發布日 ≤ 2024-01-31 來源)檢核記憶體循環是否為結構性瓶頸。≤圍欄敘事:"
    "2023 下半原廠(三星/SK 海力士/美光/西數/鎧俠)減產去化、估 2024 記憶體價落底回升(ctee "
    "2023-08-31『DRAM、NAND 明年迎春燕 台記憶體廠受惠』;MoneyDJ 研調『減產奏效、2024 Q1 DRAM "
    "跌幅料收斂』)。惟屬典型大宗商品循環(供給緊縮係原廠可逆的主動減產調節,非 ≥18 個月結構性"
    "無彈性,原廠可隨價回復產出),十銓為向三星/SK/美光寡占買晶片、組模組的下游廠——不擁稀缺節點、"
    "毛利改善屬庫存評價一次性(賺庫存財)、客戶可無痛換模組。瓶頸簽名第 2(供給無彈性)、第 3"
    "(節點控制)皆不通過,凍結提示詞明列『純大宗商品循環』為 reject 例 → reject。此為紀律性裁決:"
    "2024-01 當下 reject,與記憶體 2025-2026 超級循環(暫停報價八年首見、營收翻倍、DRAM 半年 +340%)"
    "無關(時間圍欄,全部丟棄)。營收群其餘名(宏捷科 8086 GaAs / 虹揚 3257 / 昇佳 6732 / 祥碩 5269 / "
    "神盾 6462 相鄰等)為 IC 設計/利基/化合物半導體受惠端,沿用 fabless reject。"
)
SEMI_REV_CARRY = (
    "沿用 2024-01 revenue 半導體業(補全,記憶體重檢)之時間圍欄真搜尋裁決:記憶體循環為原廠減產"
    "驅動的典型大宗商品循環 + 下游模組組裝受惠端(賺庫存財非結構定價權),reject。本月記憶體模組名"
    "(十銓 4967 續在榜;威剛 3260 於 2024-03 起加入營收加速榜,同為模組組裝廠)同屬此裁決。營收群"
    "其餘名為 IC 設計/利基/GaAs(宏捷科 8086 等)fabless 受惠端,沿用 fabless reject。真瓶頸 CoWoS "
    "已在冊(advanced_packaging_cowos)。"
)
SEMI_REV_NOMEM = (
    "沿用半導體營收裁決:本月營收加速榜無記憶體模組名(威剛/十銓),成員為 IC 設計/ASIC/利基 fabless "
    "受惠端(世芯-KY 3661 ASIC / 宏捷科 8086 GaAs / 祥碩 5269 / 譜瑞-KY 4966 相鄰 / 世紀 5314 等),"
    "riding AI/HPC 晶片需求但不擁實體產能瓶頸節點,carry reject(fabless 受惠;真瓶頸 CoWoS 已在冊 "
    "advanced_packaging_cowos)。"
)
COMPUTER = (
    "沿用既有判定:電腦及週邊設備業動能/營收群為 AI 伺服器 ODM/整機廠與板卡代工(麗臺 2465 NVIDIA AIC / "
    "勤誠 8210 機殼 / 英業達 2356 相鄰 / 廣達系 / 緯創系 / 技嘉 2376),riding 上游 GPU/CoWoS 缺口的下游"
    "代工受惠端——不擁稀缺節點、客戶可換代工=beneficiary,真瓶頸 CoWoS 已在冊(advanced_packaging_cowos)。"
    "成員逐月輪動但同屬 AI 伺服器代工受惠類別,無新供應鏈敘事,carry reject。"
)
COMPONENTS = (
    "沿用既有判定 + GB200 機構件交叉引用:電子零組件業動能/營收群為機構/散熱/連接器/hinge/PCB-CCL "
    "受惠端(兆利 3548 / 新日興 3376 hinge、佳必琪 6197 連接器、川湖 2059 伺服器滑軌、合正 5381 PCB、"
    "台光電 2383 CCL、富世達 6805 快接頭 UQD)。判定:①機構/連接器/hinge 為 AI 伺服器單位量受惠端"
    "(beneficiary,客戶可換供應商),carry reject;②川湖 2059 為伺服器滑軌龍頭(~50% 全球 AI 伺服器"
    "滑軌市佔)惟屬機構受惠端、riding AI 伺服器單位量,非稀缺不可替代節點,beneficiary;③富世達 6805"
    "(液冷快接頭 UQD)已列在冊觀察線 ai_liquid_cooling 之 enabler_candidate(未入冊)——UQD 量產出貨"
    "要到 2025 Q2、液冷滲透率 2024 僅約 11%,對 2024 H1 圍欄屬前視,續觀察不入冊(依鐵律 1 防前視);"
    "④台光電 2383 CCL 屬 CCL-PCB defer 觀察線(≤圍欄無缺料/配額硬瓶頸)。無新增入冊,carry。"
)
COMMS = (
    "沿用既有判定:通信網路業動能/營收群含 AI 資料中心光通訊(華星光 4979 / 上詮 3363 / 聯鈞 3450 / "
    "光聖 6442 / 波若威 3163 / 前鼎 4908)與交換器/網通設備(智邦 2345 相鄰 whitebox ODM、中磊 5388 "
    "相鄰)。光通訊在 ≤圍欄無光電晶片(EML/CW-LD/VCSEL)配額或缺料之硬瓶頸第一手證據(該敘事皆 "
    "2024 下半~2026 回溯文),續 defer 不入冊;交換器/網通為 AI 資料中心受惠端 beneficiary。本月無新 "
    "≤圍欄證據,carry。"
)
INFOSVC = (
    "沿用既有判定:資訊服務業為軟體/系統整合(邁達特 6112 相鄰 / 鴻鵠 6593 / 精誠 6214 相鄰 / 零壹 "
    "3029 相鄰等),無實體產能瓶頸,框架結構性 reject(軟體服務無硬產能約束,除非有授權/認證類硬"
    "約束的當時證據——本月無)。carry。"
)
OPTO = (
    "沿用既有判定:光電業為顯示/背光/面板/光學/導線架循環(達威 5432 / 榮創 3437 / 佳能 2374 / 一詮 "
    "2486 導線架 / 先進光 3362 / 揚明光 3504 相鄰),為需求受惠或顯示循環,無稀缺節點控制、供給可擴,"
    "不符瓶頸簽名。成員逐月輪動但同屬顯示/光學循環類別,carry reject。"
)
OTHERELEC = (
    "沿用既有判定:其他電子業為廠務工程/潔淨室(信紘科 6667 / 亞翔 6139 / 漢科 3402 相鄰)= 半導體 "
    "capex 受惠端 beneficiary reject;含 AI 機器視覺(所羅門 2359,AI 受惠端無稀缺節點)、AI 伺服器 "
    "ODM(鴻海 2317,整機代工 beneficiary)、CoWoS 相鄰濕製程(弘塑 3131 TPEx 已在 advanced_packaging_"
    "cowos enabler 層追蹤、不可交易)。均無獨立稀缺節點,carry reject。"
)
BIOTECH = (
    "沿用既有判定:生技醫療業為新藥上市/CDMO/醫美/檢測之需求成長 + 個股藥證催化(藥華藥 4743 / "
    "智擎 4162 / 麗豐-KY 4137 相鄰 / 保瑞 6472 相鄰 CDMO 等),無實體產能瓶頸、無控制稀缺節點、"
    "下游無 12 個月硬替代約束,結構性不符瓶頸簽名。成員為各自為政的個股催化,無新敘事,carry reject。"
)
FINANCE = (
    "沿用既有判定:金融保險營收 YoY 大增(富邦金 2881 / 國泰金 2882 / 中信金 2891 / 開發金 2883 等)"
    "為 2022-2023 低基期 + 利差/投資/防疫險回沖循環,無供給節點、無稀缺不可替代資產,結構性 reject"
    "(金融循環無實體產能瓶頸,框架同軟體/服務業)。carry。"
)
DISTRIB = (
    "沿用 2024-02 電子通路業(momentum)完整檢核之 reject 裁決:IC 通路商(亞矽 6113 / 擎亞 8096 / "
    "尚立 3360 / 蔚華科 3055 / 大聯大相鄰 / 文曄相鄰)為 AI/半導體流量之代理配銷受惠端 + 整併題材,"
    "不擁產能/認證/專利等稀缺節點、通路高度競爭可換代理、毛利結構性偏薄(轉嫁非擴張),三成員測試"
    "全指向 beneficiary,結構性 reject(通路業無實體產能瓶頸)。carry。"
)


def carry(industry: str, seed_type: str, narrative: str, theme_id=None) -> dict:
    return {
        "industry": f"{industry}(補全)",
        "seed_type": seed_type,
        "verdict": "carry_over",
        "narrative": narrative,
        "theme_id": theme_id,
        "members": [],
        "evidence": [],
        "backfill": BACKFILL,
    }


# ---- 記憶體完整重檢(真搜尋,時間圍欄 2024-01-31)----
MEM_QUERIES = [
    "DRAM NAND 記憶體 合約價 現貨價 2024年1月 三星 美光 減產 落底回升",
    "威剛 十銓 記憶體模組 2024年1月 營收 DRAM 漲價 報價",
    "DRAM contract price January 2024 Samsung Micron SK Hynix production cut recovery memory",
]
MEM_SOURCES_USED = [
    {"date": "2023-08-31", "publisher": "工商時報 ctee (20230831700587)",
     "claim": "DRAM、NAND 明年(2024)迎春燕、台記憶體廠受惠——2023 下半原廠減產,估 2024 記憶體價落底回升。"},
    {"date": "約 2023Q4~2024-01 (≤圍欄)", "publisher": "MoneyDJ 研調 / TrendForce",
     "claim": "減產奏效、2024 Q1 DRAM 價格跌幅料收斂;2023 Q4 業界估 DRAM 合約價 +5%、漲勢延續 2024;三星/SK/美光/西數/鎧俠五大原廠減產去化庫存。"},
]
MEM_DISCARDED = (
    "約 20+ 筆晚於 2024-01-31 圍欄之 2024 下半~2026 回溯文丟棄,不得作 2024-01 判斷依據:威剛/十銓"
    "暫停報價八年首見(money.udn 9055840,story id 2025-2026)、威剛 2026-01 營收 84 億 +199%/十銓 "
    "+112%(nextapple 20260207)、DRAM 半年飆 340%(ctee 20260503)、64GB RDIMM 450→900 美元(2026)、"
    "Q3 合約價 +20%/NAND +35-40%(ctee 20260705)、SK Hynix 取消長約價格上限/三星 Q3 +20%、記憶體"
    "模組雙雄獲利爆發(money.udn 9470322)等;cnyes 5553838「威剛低價庫存優勢」story id 比對約 2024 "
    "年中(> 圍欄),日期不確定,保守丟棄。"
)
MEM_BOTTLENECK = {
    "demand": "記憶體需求由 PC/伺服器庫存回補 + 2024 落底回升(≤圍欄 ctee 2023-08-31),"
              "但 2024-01 漲勢主因原廠減產去化,非 TAM 量級爆發",
    "supply_rigidity": "不通過——供給緊縮係三星/SK/美光/西數/鎧俠可逆的主動減產(commodity 循環),"
                       "非 ≥18 個月結構性無彈性,原廠可隨價回復產出",
    "concentration": "晶片端原廠寡占(高),但台廠十銓/威剛為下游模組組裝、不控制稀缺節點",
    "amplifier": "原廠減產紀律 + AI/PC 回補(有,但屬大宗商品循環放大器,非硬約束)",
}
MEM_INVALID = (
    "本即 commodity 循環判 reject;若日後出現 ≤圍欄之結構性(非減產可逆)硬缺口再重估。"
    "2024 H1 內未出現;2025-2026 記憶體超級循環屬圍欄外、不回填。"
)


def mem_reject() -> dict:
    return {
        "industry": "半導體業(補全,記憶體重檢)",
        "seed_type": "revenue",
        "verdict": "reject",
        "reject_reason": SEMI_REV_MEM_REASON,
        "narrative": "記憶體模組廠十銓 4967 列半導體營收加速榜,以時間圍欄真搜尋(≤ 2024-01-31)檢核"
                     "記憶體循環是否為結構性瓶頸:結論為原廠減產驅動的典型大宗商品循環 + 下游模組"
                     "組裝受惠端(賺庫存財),reject。此為 2023H2 記憶體重檢之直接延續(同判例)。"
                     "半導體營收群其餘名為 IC 設計/利基/GaAs fabless 受惠端,沿用 fabless reject。",
        "bottleneck_check": MEM_BOTTLENECK,
        "theme_id": None,
        "theme_name": None,
        "members": [
            {"code": "4967", "name": "十銓", "role": "beneficiary", "conviction": 0,
             "rationale": "記憶體模組組裝,大宗商品循環受惠、賺庫存評價財非結構定價權,非稀缺節點,不入冊。"},
        ],
        "conviction_updates": [],
        "invalidation_criteria": MEM_INVALID,
        "evidence": [
            {"date": "2023-08-31", "source": "ctee 20230831700587",
             "claim": "DRAM、NAND 明年迎春燕、台記憶體廠受惠(2023 下半原廠減產、估 2024 落底回升)。"},
            {"date": "約 2023Q4~2024-01", "source": "MoneyDJ 研調 / TrendForce",
             "claim": "減產奏效、2024 Q1 DRAM 跌幅料收斂;五大原廠減產去化庫存。"},
        ],
        "backfill_queries": MEM_QUERIES,
        "backfill_sources_used": MEM_SOURCES_USED,
        "backfill_discarded_note": MEM_DISCARDED,
        "backfill": BACKFILL,
    }


RECORDS: dict[str, list[dict]] = {
    "2024-01": [
        carry("半導體業", "momentum", SEMI_ICDESIGN),
        carry("電腦及週邊設備業", "momentum", COMPUTER),
        carry("電子零組件業", "momentum", COMPONENTS),
        carry("通信網路業", "momentum", COMMS),
        carry("資訊服務業", "momentum", INFOSVC),
        carry("光電業", "momentum", OPTO),
        carry("其他電子業", "momentum", OTHERELEC),
        mem_reject(),  # revenue 半導體業 完整重檢
        carry("電腦及週邊設備業", "revenue", COMPUTER),
        carry("生技醫療業", "revenue", BIOTECH),
        carry("電子零組件業", "revenue", COMPONENTS),
    ],
    "2024-02": [
        carry("半導體業", "momentum", SEMI_ICDESIGN),
        carry("光電業", "momentum", OPTO),
        carry("通信網路業", "momentum", COMMS),
        carry("電腦及週邊設備業", "momentum", COMPUTER),
        carry("電子零組件業", "momentum", COMPONENTS),
        carry("其他電子業", "momentum", OTHERELEC),
        carry("資訊服務業", "momentum", INFOSVC),
        carry("半導體業", "revenue", SEMI_REV_CARRY),
        carry("通信網路業", "revenue", COMMS),
        carry("電腦及週邊設備業", "revenue", COMPUTER),
        carry("金融保險", "revenue", FINANCE),
        carry("生技醫療業", "revenue", BIOTECH),
    ],
    "2024-03": [
        carry("半導體業", "momentum", SEMI_ICDESIGN),
        carry("電子零組件業", "momentum", COMPONENTS),
        carry("通信網路業", "momentum", COMMS),
        carry("電腦及週邊設備業", "momentum", COMPUTER),
        carry("電子通路業", "momentum", DISTRIB),
        carry("光電業", "momentum", OPTO),
        carry("其他電子業", "momentum", OTHERELEC),
        carry("半導體業", "revenue", SEMI_REV_CARRY),
        carry("通信網路業", "revenue", COMMS),
        carry("電子零組件業", "revenue", COMPONENTS),
        carry("電腦及週邊設備業", "revenue", COMPUTER),
        carry("金融保險", "revenue", FINANCE),
        carry("生技醫療業", "revenue", BIOTECH),
        carry("電子通路業", "revenue", DISTRIB),
        carry("光電業", "revenue", OPTO),
        carry("其他電子業", "revenue", OTHERELEC),
    ],
    "2024-04": [
        carry("其他電子業", "momentum", OTHERELEC),
        carry("電子零組件業", "momentum", COMPONENTS),
        carry("光電業", "momentum", OPTO),
        carry("電子通路業", "momentum", DISTRIB),
        carry("半導體業", "momentum", SEMI_ICDESIGN),
        carry("電腦及週邊設備業", "momentum", COMPUTER),
        carry("通信網路業", "momentum", COMMS),
        carry("半導體業", "revenue", SEMI_REV_NOMEM),
        carry("通信網路業", "revenue", COMMS),
        carry("生技醫療業", "revenue", BIOTECH),
        carry("光電業", "revenue", OPTO),
        carry("金融保險", "revenue", FINANCE),
        carry("電子零組件業", "revenue", COMPONENTS),
        carry("其他電子業", "revenue", OTHERELEC),
        carry("電腦及週邊設備業", "revenue", COMPUTER),
    ],
    "2024-05": [
        carry("其他電子業", "momentum", OTHERELEC),
        carry("通信網路業", "momentum", COMMS),
        carry("電子零組件業", "momentum", COMPONENTS),
        carry("電腦及週邊設備業", "momentum", COMPUTER),
        carry("光電業", "momentum", OPTO),
        carry("半導體業", "revenue", SEMI_REV_CARRY),
        carry("電子零組件業", "revenue", COMPONENTS),
        carry("光電業", "revenue", OPTO),
        carry("電腦及週邊設備業", "revenue", COMPUTER),
        carry("電子通路業", "revenue", DISTRIB),
        carry("生技醫療業", "revenue", BIOTECH),
        carry("通信網路業", "revenue", COMMS),
        carry("其他電子業", "revenue", OTHERELEC),
        carry("金融保險", "revenue", FINANCE),
    ],
    "2024-06": [
        carry("其他電子業", "momentum", OTHERELEC),
        carry("光電業", "momentum", OPTO),
        carry("電子零組件業", "momentum", COMPONENTS),
        carry("通信網路業", "momentum", COMMS),
        carry("半導體業", "revenue", SEMI_REV_CARRY),
        carry("電腦及週邊設備業", "revenue", COMPUTER),
        carry("生技醫療業", "revenue", BIOTECH),
        carry("電子零組件業", "revenue", COMPONENTS),
        carry("金融保險", "revenue", FINANCE),
        carry("通信網路業", "revenue", COMMS),
        carry("光電業", "revenue", OPTO),
        carry("電子通路業", "revenue", DISTRIB),
    ],
}


def main() -> None:
    total = 0
    for month, recs in RECORDS.items():
        path = LR / f"{month}.json"
        data = json.loads(path.read_text(encoding="utf-8"))
        existing = {
            (c.get("industry"), c.get("seed_type"))
            for c in data["clusters"]
            if c.get("backfill") == BACKFILL
        }
        added = 0
        for r in recs:
            if (r["industry"], r["seed_type"]) in existing:
                continue
            data["clusters"].append(r)
            added += 1
        path.write_text(json.dumps(data, ensure_ascii=False, indent=1) + "\n", encoding="utf-8")
        total += added
        print(f"{month}: +{added} 補全記錄 (clusters 總數 {len(data['clusters'])})")
    print(f"\n共補全 {total} 項 (79 carry_over + 1 記憶體完整重檢)。")


if __name__ == "__main__":
    main()
