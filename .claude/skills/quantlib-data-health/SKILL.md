---
name: quantlib-data-health
description: Use this skill when the user requests data integrity audit (e.g. "檢查資料", "audit data", "sanity check", "這個 anomaly 是 bug 嗎", "有沒有新的資料 bug"). Runs research/02-05 audit scripts in parallel, cross-references memory of known fixed-bugs + real-edge-cases, classifies remaining findings as actionable new bugs. Does NOT apply fixes — outputs a plan for user approval.
---

# Data integrity audit workflow

## Step 0: Memory pre-load

Read both memory files before running audits:
- `project_data_bug_history.md` — fixed bugs with commit refs; any anomaly matching these is already resolved
- `project_data_real_edge_cases.md` — patterns that LOOK like bugs but are real TWSE data (Saturday sessions, financial-sector negatives, etc.)

## Step 1: Run four audit scripts in parallel

Single Bash with `&` or invoke sequentially if parallelism complicates log parsing:

```bash
cd /Users/zaoldyeck/Documents/scala/quantlib/research
uv run python 02_anomaly_scan.py --min-stocks 20 > /tmp/audit_02.log 2>&1
uv run python 03_full_data_audit.py             > /tmp/audit_03.log 2>&1
uv run python 04_cross_verify.py                > /tmp/audit_04.log 2>&1
uv run python 05_revenue_audit.py               > /tmp/audit_05.log 2>&1
```

## Step 2: Classify every flagged anomaly

For each row flagged by any script, produce a classification:

| Classification | Criteria |
|---|---|
| **Resolved** | Matches a date / pattern in `project_data_bug_history.md` |
| **Real** | Matches a pattern in `project_data_real_edge_cases.md` |
| **Candidate** | Unknown — needs root-cause investigation |
| **New bug** | Candidate confirmed to be true error after raw CSV + live-TWSE cross-check |

## Step 3: Diagnose Candidate/New-bug

For each Candidate:

1. **Raw CSV inspection**:
   ```bash
   iconv -f BIG5 -t UTF-8 data/<table>/<market>/<year>/<date>.csv | head -5
   ```

2. **Live-TWSE cross-check**:
   ```bash
   curl -sL "https://www.twse.com.tw/rwd/zh/afterTrading/<endpoint>?..." -A "Mozilla/5.0" | iconv -f BIG5 -t UTF-8 | head -5
   ```

3. **Reader-code audit**: grep for case clauses in `src/main/scala/reader/TradingReader.scala` or `FinancialReader.scala` to see if column count changed but code didn't catch up.

## Step 4: Propose fix (do NOT apply)

For each New bug, draft a specific fix plan:

- **CSV schema drift**: `TradingReader.scala` / `FinancialReader.scala` explicit `case N =>` update with diff
- **Partial/stale publish**: `psql "DELETE FROM <table> WHERE date=X"; rm data/<table>/<market>/<year>/<date>.csv; sbt "runMain Main pull <target>"; sbt "runMain Main read <target>"`
- **Filename-content mismatch**: same delete + re-pull

## Step 5: Report (Traditional Chinese)

- **Audit 總覽**: 四個 script 各發現幾筆 anomaly
- **分類表**:
  ```
  Resolved:   X (已知修過)
  Real:       Y (真實資料邊緣)
  Candidate:  Z (待驗證)
  New bug:    W (需修復)
  ```
- **New bug 清單**: 每筆包含 root cause + fix plan
- **Fix 優先順序**: 依影響 v4 baseline 程度排序
- **Memory update 建議**: 修完 new bug 後應 append 到 `project_data_bug_history.md` 的行

## Step 6: Await user approval

Do NOT apply fixes automatically. Present the plan, wait for explicit user confirmation ("yes, apply fix A") before running any DELETE / re-pull command.

## Anti-patterns

- Don't re-diagnose known-resolved bugs (waste of context)
- Don't flag real edge cases as bugs
- Don't apply fixes without user approval
- Every proposed fix must include exact DELETE + re-import commands (copy-paste ready)
- After a fix is applied + verified, instruct user to append to memory `project_data_bug_history.md`
