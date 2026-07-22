# Serenity 交易系統 — 獨立目錄(single source of truth)

現役單一交易策略 `ev_v2_thesis_inst` 的完整家:程式、註冊表、live 狀態集中於此;
文件在 `docs/serenity/`;skill(哲學與每日清單)在 `.claude/skills/serenity-trading-system/`。

## 佈局

| 路徑 | 內容 |
|---|---|
| `daily.py` | **每日營運入口**:`uv run --project research python -m research.serenity.daily run`(資料刷新 → 引擎 → live book → 訂單計畫 → 日報);`override --force-exit <code> --reason ...` = 人工論點停損 |
| `engine.py` | 事件引擎(回測 + live 訊號;`--live-revenue` = 事件驅動月營收滾動計分) |
| `replay_2025.py` | 共用 PIT 載入器/計分器(engine 的基底;產業論點 replay) |
| `validate.py` / `execution_test.py` | 驗證電池(WF/DSR/PBO/置換)/ 富邦 realistic 路考 |
| `allocation_study.py` | Serenity × Iter95 配置研究(目前不啟用配置,單一策略) |
| `valuation_replay_2025.py` / `style_replay_2025.py` | 歷史估值/風格 replay(研究參考) |
| `registry/thesis_registry_2025.csv` | **論點註冊表(alpha 源頭)**,v2 schema 含 evidence/invalidation;維護規則見 `docs/serenity/serenity_curation_sop.md` |
| `registry/backcast_2020_2022.csv` | 2020-22 回溯策展(哲學泛化裁判用) |
| `state/live_positions.json` | live 部位帳(含收養持股的計畫與論點註記) |
| `state/overrides.json` | 人工論點停損 audit log |
| `launchd/` | 機械段排程模板 |

## 產出位置(pipeline 慣例,不在本目錄)

- 引擎輸出:`var/out/strat_lab/serenity_event_engine_v1_*`(state/book/target_weights/picks——registry 的 target_weights_path 指向此處)
- 日報/訂單計畫:`var/out/trading/{briefs,plans}/`
- 營收首見日:`research/records/revenue_first_seen.parquet`
- 文件:`docs/serenity/`(戰役報告、trials ledger、curation SOP、驗證、每日推薦)

## 鐵律

任何引擎變更先在 `docs/serenity/serenity_engine_trials_ledger.md` 預註冊;
送單永遠是使用者的人工步驟(FUBON_DRY_RUN 三閘)。
