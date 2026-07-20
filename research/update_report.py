import re

with open('/Users/zaoldyeck/Documents/scala/quantlib/research/reports/serenity_daily_report_20260717.md', 'r') as f:
    content = f.read()

thesis_6223 = """### 🏆 6223 旺矽 - 全球探針卡與 AI 晶片測試咽喉

#### 🌍 全局地緣與產業版圖 (Macro & Geopolitical Landscape)
*   **AI 算力狂潮下的終極防線與晶片測試的軍備競賽:** 隨著生成式 AI 模型的參數呈現指數級增長，全球 CSP（雲端服務供應商）與頂尖 IC 設計公司正投入巨額資本開發更強大、架構更複雜的 AI 加速器。在晶片複雜度與電晶體密度達到史無前例高峰的今日，如何確保這些造價高昂的矽晶片在進入封裝前是完美無瑕的，成為了整個半導體產業鏈的重中之重。晶圓測試（Wafer Sort）是這道防線的守門員，而探針卡（Probe Card）則是這道防線上最關鍵的實體介面。旺矽身為全球頂尖的探針卡製造商，精準卡位了 AI 晶片測試的十字路口。它不僅是傳統消費性電子的測試耗材供應商，更已經深度整合進全球最高階 AI ASIC、GPU 與高頻寬記憶體（HBM）的供應鏈中，成為支撐全球算力爆發不可或缺的測試底座與咽喉點。

#### 🧱 咽喉點與護城河解構 (Chokepoint & Moat Deconstruction)
*   **微米級測試精度物理極限與深度垂直整合的絕對壁壘:** 旺矽的護城河建立在極端物理環境下的精密加工與材料科學之上。AI 晶片具備高腳數、高頻寬與極高功耗的特性，這要求探針卡在極小的面積內植入數萬根細如髮絲的探針，且必須在極端溫度下保持穩定的接觸與完美的訊號傳輸。這是一項涉及微機電系統（MEMS）與高難度 PCB 載板設計的終極物理挑戰。旺矽掌握了從最前端探針針頭自製、載板設計到後段組裝測試的「垂直整合」能力，這不僅確保了良率與交期，更讓競爭對手難以在短時間內複製其成本結構與客製化彈性。此外，打入巨頭的高階產品測試供應鏈，需要經歷漫長且嚴苛的設計導入與可靠度驗證，這種深厚的信任機制構建了極高的客戶轉換成本。

#### 💰 價值捕獲與財務擴張路徑 (Value Capture Trajectory)
*   **高階 MEMS 探針卡放量與產品組合優化的利潤乘數效應:** 旺矽的價值捕獲機制清晰且具備爆發力。過去探針卡市場受制於傳統消費性電子的景氣循環；如今隨著高階 AI 晶片與 HBM 需求的噴發，市場對高單價、高毛利的垂直探針卡（VPC）與 MEMS 探針卡的需求急遽攀升。旺矽憑藉其技術優勢，享有強大的定價權與客製化溢價。其財務擴張路徑在於「產品結構的質變」：隨著低毛利產品佔比下降，高階測試介面出貨比重的大幅拉升，將直接帶來毛利率與營業利益率的強勁擴張。高達 58.49% 的營收年增率與 37.9% 的淨利年增率，已經證明了其收割 AI 測試規格紅利的能力。

#### 💣 潛在破口與反身性風險 (Vulnerabilities & Reflexive Risks)
*   **競爭對手技術超車與先進封裝路線改變的反身性威脅:** 秉持最高級別的防禦性悲觀，旺矽面臨的反身性風險在於測試技術路線的顛覆與紅海競爭。雖然旺矽目前在 VPC 與 MEMS 探針卡領域佔據領先地位，但全球競爭對手同樣具備雄厚的研發實力。若競爭對手率先突破下一代更高頻、更高密度的測試技術，或發起殘酷的價格戰，旺矽的超額利潤將遭到嚴重擠壓。其次，先進封裝技術的演進如果改變了測試節點的配置，例如大幅減少晶圓級測試的需求，轉而增加系統級測試的比重，這可能對探針卡的整體消耗量產生負面衝擊。此外，若全球 AI 資本支出放緩，旺矽的訂單能見度將瞬間反轉。

#### 📊 量化屠宰數據 (Quantitative Diagnostics)
*   **Adj_PEG (核心估值):** 2.582 (估值偏高)
*   **TTM 毛利率:** 50.0% (估計)
*   **營收 YoY:** 58.49%
*   **淨利 YoY:** 37.9%
*   **現金流底線:** ✅ 健康

#### 🎯 實戰價格指令與資金效率 (Execution & Velocity)
*   **【現價】:** 6220 元
*   **【停損防禦】:** 5287 元 (嚴格 -15% 防線)
*   **【雙軌目標價】:**
    *   **🤖 AI 護城河估值 (主驅動):** 8500 元 (潛在空間: 36.65%)
    *   **📊 量化模型估值 (僅供參考):** 9339.37 元
*   **【VoC 資金流轉率 (年化 APY)】:**
    *   **預估天數:** 90 天
    *   **VoC (APY) 分數:** 148.66%
    *   **資金效率與催化劑剖析 (Velocity & Catalyst Thesis):** 旺矽作為全球高階探針卡龍頭，受惠於 AI 晶片及 HBM 的強勁測試需求，展現出極強的營收與獲利動能。雖然量化估值顯示其 PEG 偏高，但其掌握的實體測試瓶頸賦予了強大的 AI 護城河溢價。給予 90 天的達標時程，是因為半導體測試介面的拉貨動能與客戶新晶片量產時程緊密相關，通常在數個月內即可見到明顯的營收挹注。其 VoC 年化 APY 達 148.66%，顯示出資金對於其高毛利產品線放量的高度期待。這 90 天內，隨著 AI 晶片新品持續推進，其在先進封裝測試領域的佈局將成為推動估值進一步重估的核心催化劑。
"""

content = re.sub(r'\*\s+\*\*🏆 6223 旺矽:\*\*.*?\n', '', content)

tier1_match = re.search(r'(## 🟢 第一梯隊：重倉 ALL-IN 區\n\n)(.*?)(?=\n## 🟡 第二梯隊：試單雷達區)', content, flags=re.DOTALL)
tier2_match = re.search(r'(## 🟡 第二梯隊：試單雷達區\n\n)(.*?)(?=\n## 💀 量化屠宰場 \(警告區\))', content, flags=re.DOTALL)
slaughter_match = re.search(r'(## 💀 量化屠宰場 \(警告區\)\n.*)', content, flags=re.DOTALL)

def sort_tier(tier_text):
    stocks = re.split(r'\n(?=### 🏆)', tier_text)
    stocks = [s for s in stocks if s.strip()]
    
    parsed_stocks = []
    for s in stocks:
        voc_match = re.search(r'\*\*VoC \(APY\) 分數:\*\* ([\d\.]+)%', s)
        voc = float(voc_match.group(1)) if voc_match else 0
        parsed_stocks.append((voc, s))
        
    parsed_stocks.sort(key=lambda x: x[0], reverse=True)
    return '\n\n'.join([s[1].strip() for s in parsed_stocks])

tier1_sorted = sort_tier(tier1_match.group(2))
tier2_text = tier2_match.group(2) + '\n\n' + thesis_6223
tier2_sorted = sort_tier(tier2_text)

new_content = content[:tier1_match.start(2)] + tier1_sorted + "\n\n## 🟡 第二梯隊：試單雷達區\n\n" + tier2_sorted + "\n\n" + slaughter_match.group(1)

with open('/Users/zaoldyeck/Documents/scala/quantlib/research/reports/serenity_daily_report_20260717.md', 'w') as f:
    f.write(new_content)

print("Report successfully updated!")
