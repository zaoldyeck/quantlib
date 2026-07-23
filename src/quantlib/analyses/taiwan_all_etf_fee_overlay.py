"""Add fee and holding-cost columns to the broad Taiwan ETF decision ranking.

Prerequisites:
  uv run --project . python -m quantlib.crawl.update


This script intentionally does not recompute historical returns.  The input
ranking already uses total-return-equivalent adjusted prices, so historical
performance is net of fund expenses.  The fee columns added here are for
forward holding-cost comparison and should not be subtracted again from the
reported historical return columns.
"""
from __future__ import annotations

import concurrent.futures
import json
import math
import re
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import polars as pl
import requests
from quantlib import paths


ROOT = Path(__file__).resolve().parents[2]
OUT_DIR = paths.OUT
INPUT_CSV = OUT_DIR / "taiwan_all_equity_etf_decision_rank.csv"
OUTPUT_CSV = OUT_DIR / "taiwan_all_equity_etf_decision_rank_with_fees.csv"
OUTPUT_MD = ROOT / "docs" / "taiwan_all_etf_ranking_with_fees.md"
RUN_DATE = datetime.now(ZoneInfo("Asia/Taipei")).strftime("%Y%m%d")
ETFINFO_SNAPSHOT = OUT_DIR / f"etfinfo_fee_snapshot_{RUN_DATE}.json"

CAPITAL_TWD = 1_400_000.0
REQUEST_HEADERS = {"User-Agent": "Mozilla/5.0"}


@dataclass(frozen=True)
class FeeRecord:
    code: str
    management_fee_pct: float | None
    custody_fee_pct: float | None
    annual_basic_fee_pct: float | None
    fee_complete: bool
    fee_source: str
    fee_note: str
    etfinfo_type: str | None = None
    etfinfo_synced_at: str | None = None


# Active ETF fees are maintained from prospectus summaries used by
# active_etf_ladder.py.  Values are decimal percentages, e.g. 1.120 means 1.120%.
ACTIVE_TOTAL_FEE_PCT: dict[str, float] = {
    "00980A": 0.785,
    "00981A": 1.120,
    "00982A": 0.835,
    "00984A": 0.740,
    "00985A": 0.485,
    "00986A": 1.250,
    "00987A": 0.785,
    "00988A": 1.520,
    "00990A": 1.050,
    "00991A": 1.040,
    "00992A": 1.235,
    "00993A": 0.740,
    "00994A": 0.735,
    "00995A": 0.785,
    "00996A": 0.785,
    "00400A": 0.785,
    "00401A": 0.785,
    "00999A": 0.785,
}


# Official or prospectus-derived fallbacks for cases where ETFInfo exposes a
# tiered fee as null or has no structured page for the code.
FEE_OVERRIDES: dict[str, FeeRecord] = {
    "0050": FeeRecord(
        code="0050",
        management_fee_pct=0.0745,
        custody_fee_pct=0.0300,
        annual_basic_fee_pct=0.1045,
        fee_complete=True,
        fee_source="Yuanta official",
        fee_note="元大官網揭露有效經理費約0.0745%(2026-05-22)；保管費用0.03%上限估算。",
    ),
    "00735": FeeRecord(
        code="00735",
        management_fee_pct=0.3500,
        custody_fee_pct=0.1300,
        annual_basic_fee_pct=0.4800,
        fee_complete=True,
        fee_source="Cathay prospectus/monthly report fallback",
        fee_note="ETFInfo 管理費為空；用公開資料揭露經理費0.35%、保管費0.13%。",
    ),
    "00928": FeeRecord(
        code="00928",
        management_fee_pct=0.4000,
        custody_fee_pct=0.0350,
        annual_basic_fee_pct=0.4350,
        fee_complete=True,
        fee_source="CTBC prospectus fallback",
        fee_note="ETFInfo 無結構化資料；用公開說明書/投資標的說明書揭露經理費0.40%、保管費0.035%。",
    ),
    "006201": FeeRecord(
        code="006201",
        management_fee_pct=0.7500,
        custody_fee_pct=0.1600,
        annual_basic_fee_pct=0.9100,
        fee_complete=True,
        fee_source="Yuanta official fallback",
        fee_note="ETFInfo 無結構化資料；元大官網揭露經理費0.75%、保管費0.16%。",
    ),
    "00939": FeeRecord(
        code="00939",
        management_fee_pct=0.3000,
        custody_fee_pct=0.0350,
        annual_basic_fee_pct=0.3350,
        fee_complete=True,
        fee_source="prospectus fallback",
        fee_note="ETFInfo 費率為空；用公開說明書常見揭露經理費0.30%、保管費0.035%。",
    ),
}


def _to_pct(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        x = float(value)
        return x if math.isfinite(x) else None
    text = str(value).strip().replace("％", "%")
    if not text or text in {"-", "—", "None", "null"}:
        return None
    match = re.search(r"([0-9]+(?:\.[0-9]+)?)\s*%?", text)
    if not match:
        return None
    return float(match.group(1))


def _nuxt_value(arr: list[Any], obj: dict[str, Any], key: str) -> Any:
    value = obj.get(key)
    if isinstance(value, int) and 0 <= value < len(arr):
        return arr[value]
    return value


def _fetch_etfinfo(code: str) -> FeeRecord:
    try:
        response = requests.get(
            f"https://www.etfinfo.tw/etf/{code}",
            headers=REQUEST_HEADERS,
            timeout=20,
        )
        response.raise_for_status()
        match = re.search(
            r'<script type="application/json"[^>]*id="__NUXT_DATA__"[^>]*>(.*?)</script>',
            response.text,
            re.S,
        )
        if not match:
            return FeeRecord(code, None, None, None, False, "ETFInfo", "no_nuxt_payload")
        arr = json.loads(match.group(1))
        data = next(
            (item for item in arr if isinstance(item, dict) and "managementFee" in item),
            None,
        )
        if data is None:
            return FeeRecord(code, None, None, None, False, "ETFInfo", "no_fee_object")

        management = _to_pct(_nuxt_value(arr, data, "managementFee"))
        custody = _to_pct(_nuxt_value(arr, data, "custodyFee"))
        annual = (
            management + custody
            if management is not None and custody is not None
            else None
        )
        return FeeRecord(
            code=code,
            management_fee_pct=management,
            custody_fee_pct=custody,
            annual_basic_fee_pct=annual,
            fee_complete=annual is not None,
            fee_source="ETFInfo",
            fee_note="ETFInfo structured managementFee + custodyFee",
            etfinfo_type=_nuxt_value(arr, data, "type"),
            etfinfo_synced_at=_nuxt_value(arr, data, "syncedAt"),
        )
    except Exception as exc:  # noqa: BLE001 - report source failures in output.
        return FeeRecord(code, None, None, None, False, "ETFInfo", f"fetch_error: {exc}")


def _load_fee_snapshot(codes: list[str]) -> dict[str, FeeRecord]:
    if ETFINFO_SNAPSHOT.exists():
        raw = json.loads(ETFINFO_SNAPSHOT.read_text(encoding="utf-8"))
        records = {
            code: FeeRecord(**payload)
            for code, payload in raw.get("records", {}).items()
            if code in codes
        }
        if len(records) == len(codes):
            return records

    with concurrent.futures.ThreadPoolExecutor(max_workers=16) as executor:
        fetched = list(executor.map(_fetch_etfinfo, codes))

    records = {record.code: record for record in fetched}
    ETFINFO_SNAPSHOT.write_text(
        json.dumps(
            {
                "created_at": datetime.now(UTC).isoformat(),
                "source": "https://www.etfinfo.tw/",
                "records": {code: asdict(record) for code, record in records.items()},
            },
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    return records


def _apply_overrides(record: FeeRecord) -> FeeRecord:
    if record.code in ACTIVE_TOTAL_FEE_PCT:
        total = ACTIVE_TOTAL_FEE_PCT[record.code]
        return FeeRecord(
            code=record.code,
            management_fee_pct=record.management_fee_pct,
            custody_fee_pct=record.custody_fee_pct,
            annual_basic_fee_pct=total,
            fee_complete=True,
            fee_source="prospectus summary",
            fee_note="主動式 ETF 用專案公開說明書費率彙整；ETFInfo 常只揭露經理費或缺保管費。",
            etfinfo_type=record.etfinfo_type,
            etfinfo_synced_at=record.etfinfo_synced_at,
        )
    return FEE_OVERRIDES.get(record.code, record)


def _fee_score(values: list[float | None]) -> list[float]:
    complete = [x for x in values if x is not None and math.isfinite(x)]
    if not complete:
        return [0.0 for _ in values]
    lo = min(complete)
    hi = max(complete)
    if abs(hi - lo) < 1e-12:
        return [1.0 if x is not None else 0.0 for x in values]
    return [
        0.0 if x is None or not math.isfinite(x) else 1.0 - ((x - lo) / (hi - lo))
        for x in values
    ]


def _fmt_pct(value: Any, digits: int = 2) -> str:
    if value is None:
        return "-"
    try:
        x = float(value)
    except (TypeError, ValueError):
        return "-"
    if not math.isfinite(x):
        return "-"
    return f"{x:+.{digits}f}%"


def _fmt_plain_pct(value: Any, digits: int = 3) -> str:
    if value is None:
        return "-"
    try:
        x = float(value)
    except (TypeError, ValueError):
        return "-"
    if not math.isfinite(x):
        return "-"
    return f"{x:.{digits}f}%"


def _fmt_money(value: Any) -> str:
    try:
        x = float(value)
    except (TypeError, ValueError):
        return "-"
    if not math.isfinite(x):
        return "-"
    return f"{x:,.0f}"


def _write_report(df: pl.DataFrame, data_cutoff: str, fee_cutoff: str) -> None:
    top = df.sort("rank").head(40)
    active = df.filter(pl.col("class") == "active").sort("rank")

    lines = [
        "# 台股全 ETF 排名與持有成本",
        "",
        f"資料截止日：本地還原股價與歷史 KPI 到 `{data_cutoff}`；即時折溢價欄位來自 `2026-05-25`；費率快照來自 ETFInfo `{fee_cutoff}` 與少數官網/公開說明書補正。",
        "",
        "## 讀法",
        "",
        "- `rank` 是原本全 ETF 綜合排名，歷史報酬已經透過市場價格與還原股價反映基金內扣費用，因此不再把費用從歷史報酬扣第二次。",
        "- `annual_basic_fee_pct` 是經理費 + 保管費的基本年費率，用來比較未來長期持有成本；它不含基金內部交易成本、買賣價差、券商手續費與稅。",
        f"- `annual_cost_twd_1_4m` 是用 `{CAPITAL_TWD:,.0f}` 元持有一年估算的基本內扣成本。",
        "- `fee_aware_rank` 是輔助排序：保留 92% 原始綜合分數，加入 8% 低費用分數；它只用來檢查高費用是否足以改變決策，不取代原始績效排名。",
        "",
        "## 前 40 名",
        "",
        "| Rank | Fee-aware | 代號 | 名稱 | 類型 | Score | 年費 | 140萬年成本 | YTD | 1Y | 3Y | 60D | 溢價 | 費用來源 |",
        "|---:|---:|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---|",
    ]
    for row in top.iter_rows(named=True):
        lines.append(
            f"| {int(row['rank'])} | {int(row['fee_aware_rank'])} | {row['code']} | {row['name']} | "
            f"{row['class']} | {float(row['score']):.3f} | {_fmt_plain_pct(row['annual_basic_fee_pct'])} | "
            f"{_fmt_money(row['annual_cost_twd_1_4m'])} | {_fmt_pct(row['ytd_cum_pct'])} | "
            f"{_fmt_pct(row['1y_cum_pct'])} | {_fmt_pct(row['3y_cum_pct'])} | "
            f"{_fmt_pct(row['60d_cum_pct'])} | {_fmt_pct(row['premium_pct_20260525'])} | "
            f"{row['fee_source']} |"
        )

    lines.extend(
        [
            "",
            "## 主動式 ETF 在全 ETF 排名中的位置",
            "",
            "| 全ETF Rank | Fee-aware | 代號 | 名稱 | Score | 年費 | 140萬年成本 | YTD | 60D | 溢價 |",
            "|---:|---:|---|---|---:|---:|---:|---:|---:|---:|",
        ]
    )
    for row in active.iter_rows(named=True):
        lines.append(
            f"| {int(row['rank'])} | {int(row['fee_aware_rank'])} | {row['code']} | {row['name']} | "
            f"{float(row['score']):.3f} | {_fmt_plain_pct(row['annual_basic_fee_pct'])} | "
            f"{_fmt_money(row['annual_cost_twd_1_4m'])} | {_fmt_pct(row['ytd_cum_pct'])} | "
            f"{_fmt_pct(row['60d_cum_pct'])} | {_fmt_pct(row['premium_pct_20260525'])} |"
        )

    lines.extend(
        [
            "",
            "## 費用資料限制",
            "",
            "- ETFInfo 多數 ETF 有結構化經理費與保管費；少數級距型或新掛牌商品沒有完整欄位，本表用 `fee_complete=false` 標示。",
            "- 0050 使用元大官網揭露的有效經理費補正；若只看 ETFInfo 名目級距，會高估目前實際持有成本。",
            "- 若一檔 ETF 的 `annual_basic_fee_pct` 為空，代表本輪未取得完整經理費 + 保管費；不要把空值解讀成零費用。",
            "",
            "## 產出檔案",
            "",
            f"- `{OUTPUT_CSV.relative_to(ROOT)}`：完整全 ETF 排名與費用欄位。",
            f"- `{ETFINFO_SNAPSHOT.relative_to(ROOT)}`：ETFInfo 費率快照。",
        ]
    )
    OUTPUT_MD.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> None:
    ranking = pl.read_csv(INPUT_CSV)
    codes = ranking["code"].cast(pl.Utf8).to_list()
    fee_records = {
        code: _apply_overrides(record)
        for code, record in _load_fee_snapshot(codes).items()
    }

    rows: list[dict[str, Any]] = []
    for code in codes:
        record = fee_records[code]
        rows.append(
            {
                **asdict(record),
                "annual_cost_twd_1m": (
                    record.annual_basic_fee_pct / 100.0 * 1_000_000.0
                    if record.annual_basic_fee_pct is not None
                    else None
                ),
                "annual_cost_twd_1_4m": (
                    record.annual_basic_fee_pct / 100.0 * CAPITAL_TWD
                    if record.annual_basic_fee_pct is not None
                    else None
                ),
                "five_year_basic_fee_drag_pct": (
                    (1.0 - (1.0 - record.annual_basic_fee_pct / 100.0) ** 5) * 100.0
                    if record.annual_basic_fee_pct is not None
                    else None
                ),
                "ten_year_basic_fee_drag_pct": (
                    (1.0 - (1.0 - record.annual_basic_fee_pct / 100.0) ** 10) * 100.0
                    if record.annual_basic_fee_pct is not None
                    else None
                ),
            }
        )

    fees = pl.DataFrame(rows)
    joined = ranking.join(fees, on="code", how="left")

    fee_scores = _fee_score(joined["annual_basic_fee_pct"].to_list())
    joined = joined.with_columns(
        pl.Series("fee_score", fee_scores),
    ).with_columns(
        (0.92 * pl.col("score") + 0.08 * pl.col("fee_score")).alias("fee_aware_score")
    )
    fee_rank = (
        joined.sort("fee_aware_score", descending=True)
        .with_row_index("fee_aware_rank", offset=1)
        .select(["code", "fee_aware_rank"])
    )
    joined = joined.join(fee_rank, on="code", how="left").sort("rank")
    joined.write_csv(OUTPUT_CSV)

    last_date = str(joined["last_date"].drop_nulls().max())[:10]
    fee_cutoffs = [
        str(x)
        for x in joined["etfinfo_synced_at"].drop_nulls().unique().to_list()
        if str(x)
    ]
    fee_cutoff = max(fee_cutoffs) if fee_cutoffs else "unknown"
    _write_report(joined, last_date, fee_cutoff)

    missing = joined.filter(~pl.col("fee_complete"))
    print(f"read {INPUT_CSV.relative_to(ROOT)} rows={ranking.height}")
    print(f"wrote {OUTPUT_CSV.relative_to(ROOT)} rows={joined.height}")
    print(f"wrote {OUTPUT_MD.relative_to(ROOT)}")
    print(f"fee_complete={joined.height - missing.height}/{joined.height} missing={missing.height}")
    print("Top 20 with fees")
    with pl.Config(tbl_rows=22, tbl_cols=14):
        print(
            joined.select(
                [
                    "rank",
                    "fee_aware_rank",
                    "code",
                    "name",
                    "class",
                    "score",
                    "annual_basic_fee_pct",
                    "annual_cost_twd_1_4m",
                    "ytd_cum_pct",
                    "1y_cum_pct",
                    "60d_cum_pct",
                    "premium_pct_20260525",
                ]
            ).head(20)
        )
    if missing.height:
        print("Missing fee rows")
        with pl.Config(tbl_rows=30, tbl_cols=8):
            print(missing.select(["rank", "code", "name", "class", "fee_note"]).head(30))


if __name__ == "__main__":
    main()
