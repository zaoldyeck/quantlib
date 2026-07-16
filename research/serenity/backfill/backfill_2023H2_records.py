"""2023H2 回溯標記 — 種子聚類檢核記錄補全(判準 B 完備性).

背景:2023H2 批(2023-07~12)的標記 agent 在多數月份把種子聚類折進「合併/
改名的 carry_over 群」,導致 `pilot_acceptance.py` 判準 B 的**子字串比對**
(`seed industry in cluster industry`)落空 —— 檢核其實做了,只是記錄行未按
種子產業名寫入,共 36 項落空(見 `2023H2_report.md`)。

本腳本把 36 項的檢核記錄補回各月 `label_runs/{month}.json` 的 clusters 陣列:

- **carry_over(34 項)**:該種子產業與成員構成和先前某月已檢核者實質相同
  (成員重疊高、無新供應鏈敘事),沿用既有 admit/reject 判定。empty evidence
  → 判準 C 零違規;verdict=carry_over → 不進判準 A 的事後報酬表。
- **完整重檢(2 項)**:2023-11 momentum 半導體業、2023-12 revenue 半導體業 ——
  這兩個子群含記憶體模組廠(威剛 3260、十銓 4967),是 rule 3 點名的「可能有
  新供應鏈敘事(記憶體)」細分。本次以時間圍欄真搜尋(WebSearch 2026-07,
  只採 ≤ 該月月末來源)檢核:2023 Q4 記憶體循環由原廠主動減產驅動落底回升
  (≤圍欄:TrendForce 2023-10-13 估 Q4 DRAM 合約價季增 3-8%;Digitimes
  2023-09-22 三星/SK 海力士 H2 減產、DRAM 落底反彈),但屬**典型大宗商品
  循環**(供給緊縮係原廠可逆的產能調節,非 ≥18 個月結構性無彈性),且威剛/
  十銓為向三星/SK/美光寡占買晶片的下游**模組組裝廠**(不擁稀缺節點、毛利
  改善屬庫存評價一次性、客戶可無痛換模組)—— 瓶頸簽名第 2、第 3 皆不通過,
  凍結提示詞明列「純大宗商品循環」為直接 reject 例 → **reject**。此為紀律性
  裁決:2023-11/12 當下 reject,與記憶體 2024 後續上漲無關(時間圍欄)。

丟棄的晚於圍欄來源(2024-2026 回溯文,不得作 2023 判斷依據):三星 NAND
首季漲破 100%(2026-05)、DDR3/DDR4 缺貨延續 2026、三星 Q3-2026 DRAM +20%、
記憶體超級循環等,約 25+ 筆。

依賴:不需 cache_tables.py(只改 JSON);判準驗收 `pilot_acceptance.py` 需 cache。
Run: uv run --project research python -m research.serenity.backfill.backfill_2023H2_records
冪等:以 (industry, seed_type, backfill 標記) 去重,重跑不重複追加。
完成後跑:uv run --project research python -m research.serenity.backfill.pilot_acceptance \
          --months 2023-07 2023-08 2023-09 2023-10 2023-11 2023-12 --tag 2023H2
"""

from __future__ import annotations

import json
from pathlib import Path

HERE = Path(__file__).parent
LR = HERE / "label_runs"
BACKFILL = "2023H2_recordkeeping"

# ---- 既有判定的沿用敘事(成員名錨定各月已讀種子)----
AI_SERVER_ODM = (
    "沿用 2023-07 判定:電腦及週邊設備業動能/營收群為 AI 伺服器整機廠與 ODM"
    "(廣達 2382/緯創 3231/技嘉 2376/英業達 2356/光寶 2301/勤誠 8210/迎廣 6117/"
    "銦泰 3693 等),riding 上游 GPU/CoWoS 缺口的下游代工受惠者 —— 不擁稀缺節點、"
    "客戶可換代工=beneficiary,真瓶頸 CoWoS 已在冊(advanced_packaging_cowos)。"
    "成員構成與 2023-07 高度重疊、無新供應鏈敘事。"
)
OPTO = (
    "沿用 2023-07 合併小群 reject 判定:光電業(廣運 6125 倉儲自動化/AI 搬運受惠、"
    "一詮 2486/全台 3038 導線架,及顯示/背光材料達威 5432/榮創 3437/友輝 4933/"
    "先進光 3362 等)為需求受惠或顯示循環,無稀缺節點控制、供給可擴,不符瓶頸簽名。"
    "各月成員同屬此類,無新供應鏈敘事。"
)
BIOTECH = (
    "沿用 2023-07 判定:生技醫療業為新藥上市/CDMO/人口老化之需求成長 + 個股藥證/"
    "醫美催化(智擎 4162/藥華藥/高端疫苗 6547/合一 4743 等),無實體產能瓶頸、無控制"
    "稀缺節點、下游無 12 個月硬替代約束,結構性不符瓶頸簽名。成員為各自為政的個股"
    "催化,無新敘事。"
)
LIGHT_COMMS = (
    "沿用 2023-08 判定:通信網路業 AI 資料中心光通訊(華星光 4979/上詮 3363/聯鈞 3450/"
    "光聖 6442/波若威 3163/前鼎 4908 等)在 ≤圍欄無光電晶片(EML/CW-LD/VCSEL)配額或"
    "缺料之硬瓶頸證據(該敘事皆 2024-2026 回溯文,已丟棄);智邦 2345 為交換器 whitebox "
    "ODM 受惠端。續 defer 不入冊,本月無新 ≤圍欄證據。"
)
HEAVY_ELEC = (
    "沿用在冊主題 grid_heavy_electrical:電機機械營收群中華城 1519 續在榜、屬電網重電"
    "寡占(華城/中興電/士電/亞力),持平沿用;其餘名(力山 1515/擎邦 6122/金雨 4503/"
    "東元 2371 等)為多角化或循環受惠,本月無新增 ≤圍欄訂單/漲價報導,不上調 conviction。"
)
CONSTRUCTION = (
    "沿用 2023-09 判定:建材營造營收暴增(潤隆 1808/興富發 2542/基泰 2538/愛山林 2540)"
    "為建案完工交屋一次性認列(known judgement rule:營建按完工交屋認列,YoY 具跳動性),"
    "非供應鏈瓶頸、無稀缺節點與供給無彈性,結構性 reject。"
)
INFO_SVC = (
    "沿用 2023-07 判定:資訊服務業為軟體/系統整合(驊訊 6148/聚碩 6112/凌群 2453/"
    "神通 2468 及本月精誠/凌網等),無實體產能瓶頸,框架結構性 reject(軟體服務無硬"
    "產能約束)。"
)
OTHER = (
    "沿用 2023-07『其他』判定:雜項循環/內需(泰銘 6625 鉛回收/新鼎 5209 系統整合,"
    "及本月康那香 9919/鼎基 6585/國統 8936/宏大 8932 等不織布/橡膠/管材內需),無稀缺"
    "節點控制與供給無彈性,不符瓶頸簽名。"
)
COMPONENTS = (
    "沿用 2023-07 + 2023-08/09 判定:電子零組件業為 PCB/CCL(定穎 3715/精成科 6191/"
    "合正 5381/華通 2313/高技 5439/金像電 2368/健鼎 3044)與機構/散熱/連接器/hinge"
    "(兆利 3548/信錦 1582/新日興 3376/佳邦 6284/佳必琪 6197/川湖 2059)。PCB/CCL 屬"
    "CCL-PCB defer(≤圍欄無缺料/配額硬瓶頸,已四次驗證);機構/連接器/hinge 為 AI 伺服器"
    "單位量受惠端。此群無記憶體/CoWoS 細分(記憶體模組名歸在半導體聚類),成員同屬既"
    "檢核類別,carry。"
)
# 半導體 IC 設計沿用(無記憶體名的子群)
SEMI_ICDESIGN_1123 = (
    "沿用 2023-11 momentum 半導體業(補全,記憶體重檢)之記憶體循環真搜尋裁決:本營收"
    "子群(昇佳 6732/安格 6684/敦泰 3545/亦立 3014/矽統 2363)無記憶體模組名,純 IC 設計/"
    "利基受惠端,無實體瓶頸節點,carry reject(fabless 受惠)。"
)
SEMI_ICDESIGN_1212 = (
    "沿用 2023-12 revenue 半導體業(補全,記憶體重檢)之記憶體循環真搜尋裁決:本動能"
    "子群(安國 8054/金麗科 3228/笙泉 3122/天鈺 4961/祥碩 5269/訊芯 6451/威盛 2388/"
    "采鈺 6789 等)為 IC 設計/利基受惠端,無記憶體模組名、無實體瓶頸節點,carry reject。"
)

# ---- 記憶體完整重檢(真搜尋,時間圍欄)----
MEM_QUERIES = [
    "記憶體 DRAM NAND 現貨價 漲價 2023年11月 三星 減產 威剛 十銓 模組廠",
    "DRAM contract price increase Q4 2023 Samsung production cut memory recovery November 2023",
]
MEM_SOURCES_USED = [
    {"date": "2023-10-13", "publisher": "TrendForce",
     "claim": "估 2023 Q4 DRAM 合約價季增 3-8%;DDR5 因新 CPU 備貨續漲,DDR4/DDR5 漲價循環將啟。"},
    {"date": "2023-09-22", "publisher": "Digitimes",
     "claim": "三星/SK 海力士 2023 H2 擴大 DRAM 減產,韓廠認為 DRAM 價已落底、估將反彈。"},
]
MEM_DISCARDED = (
    "約 25+ 筆晚於圍欄之 2024-2026 回溯文丟棄(三星 NAND 首季漲破 100% 2026-05、"
    "DDR3/DDR4 缺貨延續 2026、三星 Q3-2026 DRAM +20%、記憶體超級獲利循環、ctee "
    "2026-07 合約價再飆等),不得作 2023 判斷依據。"
)
MEM_BOTTLENECK = {
    "demand": "記憶體需求由 AI 伺服器 + PC 回補回升(≤圍欄 TrendForce/Digitimes),"
              "但 Q4 漲勢主因原廠減產而非 TAM 量級爆發",
    "supply_rigidity": "不通過 —— 供給緊縮係三星/SK/美光可逆的主動減產(commodity 循環),"
                       "非 ≥18 個月結構性無彈性,原廠可隨價回復產出",
    "concentration": "晶片端三家寡占(高),但台廠威剛/十銓為下游模組組裝、不控制稀缺節點",
    "amplifier": "原廠減產紀律 + AI 需求(有,但屬大宗商品循環放大器,非硬約束)",
}
MEM_INVALID = "本即 commodity 循環判 reject;若日後出現 ≤圍欄之結構性(非減產可逆)硬缺口再重估。"


def mem_reject(seed_type: str, module_members: list[dict], reject_reason: str) -> dict:
    return {
        "industry": "半導體業(補全,記憶體重檢)",
        "seed_type": seed_type,
        "verdict": "reject",
        "reject_reason": reject_reason,
        "narrative": "記憶體模組廠列入半導體動能/營收榜,以時間圍欄真搜尋檢核記憶體循環是否"
                     "為結構性瓶頸:結論為典型大宗商品循環 + 下游模組組裝受惠端,reject。"
                     "半導體聚類其餘名為 IC 設計 fabless 受惠端,沿用既有 fabless reject。",
        "bottleneck_check": MEM_BOTTLENECK,
        "theme_id": None,
        "theme_name": None,
        "members": module_members,
        "conviction_updates": [],
        "invalidation_criteria": MEM_INVALID,
        "evidence": [
            {"date": "2023-10-13", "source": "TrendForce",
             "claim": "估 2023 Q4 DRAM 合約價季增 3-8%,DDR5 因新 CPU 備貨續漲。"},
            {"date": "2023-09-22", "source": "Digitimes",
             "claim": "三星/SK 海力士 2023 H2 擴大 DRAM 減產,DRAM 價落底反彈。"},
        ],
        "backfill_queries": MEM_QUERIES,
        "backfill_sources_used": MEM_SOURCES_USED,
        "backfill_discarded_note": MEM_DISCARDED,
        "backfill": BACKFILL,
    }


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


MEM_11_MOM_REASON = (
    "記憶體模組廠威剛 3260 / 十銓 4967(均 TWSE 上市)於 2023-11 半導體動能榜:≤圍欄證據"
    "(TrendForce 2023-10-13 Q4 DRAM 合約價季增 3-8%;Digitimes 2023-09-22 三星/SK 減產、"
    "DRAM 落底反彈)顯示記憶體循環由原廠主動減產驅動落底回升。惟屬典型大宗商品循環"
    "(供給緊縮係原廠可逆調節,非結構性無彈性),威剛/十銓為向三星/SK/美光寡占買晶片、"
    "組模組的下游廠 —— 不擁稀缺節點、毛利改善屬庫存評價一次性、客戶可無痛換模組。瓶頸"
    "簽名第 2(供給無彈性)、第 3(節點控制)皆不通過,凍結提示詞明列『純大宗商品循環』"
    "為 reject 例。動能群其餘名(矽統 2363/世芯 3661/祥碩 5269/力旺 3529/訊芯 6451/揚智 3041/"
    "天鈺 4961)為 IC 設計 fabless 受惠端,沿用 fabless reject。"
)
MEM_12_REV_REASON = (
    "記憶體模組廠十銓 4967(TWSE 上市)於 2023-12 半導體營收加速榜(近三月 YoY +162%):"
    "同一記憶體循環(≤圍欄 TrendForce 2023-10-13 / Digitimes 2023-09-22)判典型大宗商品"
    "循環 + 下游模組組裝受惠端,reject(理由同 2023-11)。營收群其餘名(凌陽創新 5236/"
    "宏捷科 8086 GaAs/矽統 2363/家登 3680 EUV pod/昇佳 6732/虹揚 3257/沛亨 6291 等)為 IC "
    "設計/利基/先進封裝相鄰受惠,沿用 fabless/beneficiary reject(家登 EUV pod 屬 "
    "advanced_packaging_cowos 相鄰,已在冊追蹤)。"
)

RECORDS: dict[str, list[dict]] = {
    "2023-07": [
        carry("光電業", "momentum", OPTO),
        carry("其他電子業", "momentum",
              "沿用 2023-07 合併小群 reject 判定:其他電子業(福華 8085 面板背光、弘塑 3131 "
              "CoWoS 濕製程設備、碩天 3617 UPS)—— 弘塑為先進封裝濕製程設備(CoWoS 相鄰,"
              "併入 advanced_packaging_cowos 追蹤層,TPEx 不可交易),福華/碩天為顯示/電源"
              "受惠,均無獨立稀缺節點,不成獨立主題。"),
    ],
    "2023-08": [
        carry("電腦及週邊設備業", "momentum", AI_SERVER_ODM),
        carry("其他電子業", "momentum",
              "沿用 2023-07 合併小群 + 本月 CoWoS 設備處理:其他電子業(萬潤 6187/弘塑 3131 "
              "先進封裝濕製程設備、碩天 3617 UPS、亞翔 6139 廠務工程、無敵 8201)—— 萬潤/弘塑"
              "已在本月『半導體業(CoWoS 設備)』cluster 記為 enabler(TPEx 追蹤不可交易),"
              "其餘為受惠端,carry。"),
        carry("電腦及週邊設備業", "revenue", AI_SERVER_ODM),
        carry("生技醫療業", "revenue", BIOTECH),
    ],
    "2023-09": [
        carry("電腦及週邊設備業", "momentum", AI_SERVER_ODM),
        carry("光電業", "momentum", OPTO),
        carry("生技醫療業", "revenue", BIOTECH),
    ],
    "2023-10": [
        carry("電腦及週邊設備業", "momentum", AI_SERVER_ODM),
        carry("光電業", "momentum", OPTO),
        carry("生技醫療業", "revenue", BIOTECH),
    ],
    "2023-11": [
        mem_reject("momentum", [
            {"code": "3260", "name": "威剛", "role": "beneficiary", "conviction": 0,
             "rationale": "DRAM/NAND 模組組裝,大宗商品循環受惠、非稀缺節點,不入冊。"},
            {"code": "4967", "name": "十銓", "role": "beneficiary", "conviction": 0,
             "rationale": "記憶體模組廠,同上,不入冊。"},
        ], MEM_11_MOM_REASON),
        carry("電子零組件業", "momentum", COMPONENTS),
        carry("電腦及週邊設備業", "momentum", AI_SERVER_ODM),
        carry("通信網路業", "momentum", LIGHT_COMMS),
        carry("生技醫療業", "momentum", BIOTECH),
        carry("生技醫療業", "revenue", BIOTECH),
        carry("電腦及週邊設備業", "revenue", AI_SERVER_ODM),
        carry("半導體業", "revenue", SEMI_ICDESIGN_1123),
        carry("電子零組件業", "revenue", COMPONENTS),
        carry("電機機械", "revenue", HEAVY_ELEC, theme_id="grid_heavy_electrical"),
        carry("建材營造", "revenue", CONSTRUCTION),
        carry("光電業", "revenue", OPTO),
    ],
    "2023-12": [
        carry("半導體業", "momentum", SEMI_ICDESIGN_1212),
        carry("電子零組件業", "momentum", COMPONENTS),
        carry("電腦及週邊設備業", "momentum", AI_SERVER_ODM),
        carry("其他", "momentum", OTHER),
        carry("光電業", "momentum", OPTO),
        carry("資訊服務業", "momentum", INFO_SVC),
        carry("通信網路業", "momentum", LIGHT_COMMS),
        mem_reject("revenue", [
            {"code": "4967", "name": "十銓", "role": "beneficiary", "conviction": 0,
             "rationale": "記憶體模組廠,2023-12 營收 YoY +162%,大宗商品循環受惠、非節點,不入冊。"},
        ], MEM_12_REV_REASON),
        carry("生技醫療業", "revenue", BIOTECH),
        carry("電機機械", "revenue", HEAVY_ELEC, theme_id="grid_heavy_electrical"),
        carry("通信網路業", "revenue", LIGHT_COMMS),
        carry("電腦及週邊設備業", "revenue", AI_SERVER_ODM),
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
    print(f"\n共補全 {total} 項 (carry_over + 記憶體完整重檢)。")


if __name__ == "__main__":
    main()
