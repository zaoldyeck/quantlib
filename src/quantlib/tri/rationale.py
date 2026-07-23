"""策展理由的取用層——把 LLM 當時查到的東西原封不動搬進報告(零 LLM)。

這套系統的架構就是「**把 LLM 的判斷壓成帶時間戳的資料**」,所以產報告不需要
任何 LLM:理由早就在這些檔案裡:

| 策略 | 檔案 | 內容 |
|---|---|---|
| Evergreen | `data/registry_v3.parquet` | 每月標記:題材/訊號類型/事件全文/證據(含出處日期)/失效條件/信念度 |
| Evergreen | `data/ev28_news/{month}/materials.json` | **當月搜尋材料原文**:date/source/url/title/excerpt |
| Evergreen | `data/prompts/{month}.txt` | 當月標記提示詞(含哲學) |
| Serenity | `registry/thesis_registry_2025.csv` | 主題/瓶頸層/論點原文/證據日+URL/**失效條件**/信念度/複審期限 |
| Serenity | `state/live_positions.json` | 每個部位的收養檢核註記(thesis_note) |
| S | 無(純量化,零 LLM) | 理由=因子值與排名,報告時重算 |

誠實聲明(報告會照實顯示):Serenity 註冊表 58 筆裡只有 9 筆有真實 evidence_url,
其餘是 `legacy:curated-2026H1`(早期入冊未留出處);Evergreen 的策展留存規格較完整。
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path

import polars as pl
from quantlib import paths

REPO_ROOT = Path(__file__).resolve().parents[3]
EV_REGISTRY = REPO_ROOT / "src" / "quantlib" / "evergreen" / "data" / "registry_v3.parquet"
EV_NEWS_DIRS = (REPO_ROOT / "src" / "quantlib" / "evergreen" / "data" / "ev28_news",
                REPO_ROOT / "src" / "quantlib" / "evergreen" / "data" / "ev45_news",
                REPO_ROOT / "src" / "quantlib" / "evergreen" / "data" / "ev27_news")
SER_REGISTRY = REPO_ROOT / "src" / "quantlib" / "serenity" / "registry" / "thesis_registry_2025.csv"
SER_LEDGER = paths.STATE / "serenity" / "live_positions.json"


@dataclass
class Material:
    date: str
    source: str
    title: str
    url: str
    excerpt: str


@dataclass
class EvergreenLabel:
    month: str
    name: str
    theme: str
    signal_type: str
    event: str
    evidence: str
    invalidation: str
    conviction: int
    materials: list[Material] = field(default_factory=list)


@dataclass
class SerenityThesis:
    theme_id: str
    theme_name: str
    bottleneck_layer: str
    conviction: int
    source_note: str
    evidence_date: str
    evidence_url: str
    invalidation_criteria: str
    review_by: str
    active_from: str
    thesis_note: str = ""

    @property
    def evidence_is_sourced(self) -> bool:
        """早期入冊只寫 legacy:… 沒有真出處——報告要標出來,不能假裝有證據。"""
        return bool(self.evidence_url) and not str(self.evidence_url).startswith("legacy")


def _load_ev_registry() -> pl.DataFrame:
    if not EV_REGISTRY.exists():
        return pl.DataFrame()
    return pl.read_parquet(EV_REGISTRY).with_columns(pl.col("code").cast(pl.Utf8))


def _month_materials(month: str, code: str, name: str) -> list[Material]:
    """當月搜尋材料中與這檔相關者 + 該月的宏觀 regime 判讀(它是標記的前提)。"""
    for base in EV_NEWS_DIRS:
        f = base / month / "materials.json"
        if not f.exists():
            continue
        try:
            rows = json.loads(f.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            continue
        out: list[Material] = []
        for r in rows if isinstance(rows, list) else []:
            blob = f"{r.get('title', '')}{r.get('excerpt', '')}"
            is_mine = code in blob or (name and name in blob)
            is_regime = "regime" in str(r.get("title", ""))
            if is_mine or is_regime:
                out.append(Material(date=str(r.get("date", "")), source=str(r.get("source", "")),
                                    title=str(r.get("title", "")), url=str(r.get("url", "")),
                                    excerpt=str(r.get("excerpt", ""))))
        if out:
            return out
    return []


def evergreen_labels(code: str) -> list[EvergreenLabel]:
    """該檔的全部標記史(新→舊);最近一次附當月搜尋材料原文。"""
    reg = _load_ev_registry()
    if reg.is_empty():
        return []
    rows = reg.filter(pl.col("code") == code).sort("month", descending=True).to_dicts()
    out = []
    for i, r in enumerate(rows):
        lab = EvergreenLabel(
            month=str(r["month"]), name=str(r.get("name") or ""), theme=str(r.get("theme") or ""),
            signal_type=str(r.get("signal_type") or ""), event=str(r.get("event") or ""),
            evidence=str(r.get("evidence") or ""), invalidation=str(r.get("invalidation") or ""),
            conviction=int(r.get("conviction") or 0),
        )
        if i == 0:  # 只有最近一次附材料原文——那才是「現在的理由」
            lab.materials = _month_materials(lab.month, code, lab.name)
        out.append(lab)
    return out


def serenity_thesis(code: str) -> SerenityThesis | None:
    if not SER_REGISTRY.exists():
        return None
    reg = pl.read_csv(SER_REGISTRY, schema_overrides={"company_code": pl.Utf8})
    rows = reg.filter(pl.col("company_code") == code).sort("active_from", descending=True).to_dicts()
    if not rows:
        return None
    r = rows[0]
    note = ""
    try:
        led = json.loads(SER_LEDGER.read_text(encoding="utf-8"))
        note = str((led.get("positions") or {}).get(code, {}).get("thesis_note") or "")
    except (OSError, ValueError):
        pass
    return SerenityThesis(
        theme_id=str(r.get("theme_id") or ""), theme_name=str(r.get("theme_name") or ""),
        bottleneck_layer=str(r.get("bottleneck_layer") or ""),
        conviction=int(r.get("conviction") or 0), source_note=str(r.get("source_note") or ""),
        evidence_date=str(r.get("evidence_date") or ""), evidence_url=str(r.get("evidence_url") or ""),
        invalidation_criteria=str(r.get("invalidation_criteria") or ""),
        review_by=str(r.get("review_by") or ""), active_from=str(r.get("active_from") or ""),
        thesis_note=note,
    )
