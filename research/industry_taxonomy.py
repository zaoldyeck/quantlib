"""Canonical point-in-time industry taxonomy for TW equity research.

The raw source is the official MOPS monthly operating-revenue feed.  That feed
embeds the exchange/TPEx industry label in each monthly file, but labels have
changed over time and some old files use legacy names.  Strategies should use
the normalized point-in-time taxonomy here, not `operating_revenue.industry`
directly.
"""
from __future__ import annotations

from datetime import date
from typing import Any

import polars as pl


TAXONOMY_VERSION = "tw-official-revenue-pit-v1"

COMMON_CODE_PATTERN = r"^[1-9][0-9]{3}$"

_NORMALIZE_EXACT = {
    "水泥": "水泥工業",
    "食品": "食品工業",
    "塑膠": "塑膠工業",
    "紡織": "紡織纖維",
    "塑紡(二)": "塑化紡織",
    "塑化紡織(二)": "塑化紡織",
    "電機": "電機機械",
    "電機電纜(二)": "電機電纜",
    "化工": "化學工業",
    "生物科技": "生技醫療業",
    "化學生技醫療": "化學生技醫療",
    "營建": "建材營造",
    "營建(二)": "建材營造",
    "建材營建": "建材營造",
    "水泥窯製營造": "水泥窯製營造",
    "觀光事業": "觀光餐旅",
    "觀光": "觀光餐旅",
    "運輸": "航運業",
    "鋼鐵": "鋼鐵工業",
    "橡膠": "橡膠工業",
    "汽車": "汽車工業",
    "玻璃": "玻璃陶瓷",
    "造紙": "造紙工業",
    "軟體": "資訊服務業",
    "通訊網路": "通信網路業",
    "電子(二)": "電子工業",
    "其他(二)": "其他",
    "電子商務": "數位雲端",
    "綜合企業": "綜合",
    "金融": "金融保險",
    "金融保險業": "金融保險",
    "證券": "金融保險",
}

_ELECTRONICS = {
    "電子工業",
    "電子",
    "半導體業",
    "電腦及週邊設備業",
    "光電業",
    "通信網路業",
    "電子零組件業",
    "電子通路業",
    "資訊服務業",
    "其他電子業",
    "數位雲端",
}

_FINANCIAL = {"金融", "金融業", "金融保險", "金融保險業"}

_SPECIAL = {"管理股票", "存託憑證"}


def normalize_industry_name(raw: str | None) -> str | None:
    """Normalize legacy MOPS labels to the closest official category name."""
    if raw is None:
        return None
    value = " ".join(str(raw).strip().split())
    if not value:
        return None
    if value.startswith("金融保險"):
        return "金融保險"
    return _NORMALIZE_EXACT.get(value, value)


def broad_sector(industry: str | None) -> str | None:
    """Map canonical industry labels to coarse risk buckets."""
    if industry is None:
        return None
    if industry in _ELECTRONICS:
        return "電子"
    if industry in _FINANCIAL:
        return "金融"
    if industry in {"建材營造", "水泥工業", "玻璃陶瓷"}:
        return "營建建材"
    if industry in {"食品工業", "貿易百貨", "居家生活", "觀光餐旅", "運動休閒"}:
        return "民生消費"
    if industry in {"塑膠工業", "化學工業", "橡膠工業", "油電燃氣業", "綠能環保", "塑化紡織", "化學生技醫療"}:
        return "原物料能源"
    if industry in {"紡織纖維", "電機機械", "電器電纜", "電機電纜", "鋼鐵工業", "汽車工業", "造紙工業", "航運業"}:
        return "傳產工業"
    if industry in {"生技醫療業", "文化創意業", "農業科技"}:
        return "特色產業"
    if industry in {"水泥窯製營造"}:          # 水泥 + 窯製 + 營造 → 營建建材
        return "營建建材"
    if industry in {"綜合", "其他", "其他業"}:  # 綜合企業/未細分 → 其他
        return "其他"
    if industry in _SPECIAL:
        return "特殊分類"
    # 未映射的分類**歸「其他」並告警**,不再把原名當粗分類洩漏出去(2026-07-23 FC8)。
    # 舊 fallback `return industry` 讓 27,401 列(其他)、綜合、水泥窯製營造 等直接
    # 用細分類當 broad_sector → sector 中性化/分組時多出假桶。告警讓新出現的分類被
    # 看見而非靜默漏出。
    import warnings
    warnings.warn(f"broad_sector 未映射分類「{industry}」→ 歸『其他』;"
                  "請補映射", stacklevel=2)
    return "其他"


def _schema_empty() -> pl.DataFrame:
    return pl.DataFrame(
        schema={
            "market": pl.Utf8,
            "company_code": pl.Utf8,
            "company_name": pl.Utf8,
            "source_year": pl.Int32,
            "source_month": pl.Int32,
            "source_ym": pl.Int32,
            "effective_date": pl.Date,
            "raw_industry": pl.Utf8,
            "industry": pl.Utf8,
            "broad_sector": pl.Utf8,
            "is_financial": pl.Boolean,
            "is_special_category": pl.Boolean,
            "industry_source": pl.Utf8,
            "taxonomy_version": pl.Utf8,
        }
    )


def build_industry_taxonomy_pit(con: Any) -> pl.DataFrame:
    """Build a point-in-time industry table from raw operating revenue rows.

    Effective date is set to the conservative monthly revenue publication proxy
    used elsewhere in the research stack: first day of the next month + 12 days.
    This avoids applying a classification before the source file was observable.
    """
    raw = con.sql(
        f"""
        SELECT market, type, year, month, company_code, company_name, industry
        FROM operating_revenue
        WHERE industry IS NOT NULL
          AND industry <> ''
          AND regexp_matches(company_code, '{COMMON_CODE_PATTERN}')
        """
    ).pl()
    if raw.is_empty():
        return _schema_empty()

    out = (
        raw.with_columns(
            [
                pl.when(pl.col("type") == "consolidated").then(0).otherwise(1).alias("_type_priority"),
                pl.col("industry").str.strip_chars().alias("raw_industry"),
            ]
        )
        .sort(["market", "company_code", "year", "month", "_type_priority"])
        .unique(["market", "company_code", "year", "month"], keep="first", maintain_order=True)
        .with_columns(
            [
                pl.col("raw_industry")
                .map_elements(normalize_industry_name, return_dtype=pl.Utf8)
                .alias("industry"),
                pl.date(pl.col("year"), pl.col("month"), 1)
                .dt.offset_by("1mo")
                .dt.offset_by("12d")
                .alias("effective_date"),
                (pl.col("year") * 100 + pl.col("month")).cast(pl.Int32).alias("source_ym"),
            ]
        )
        .with_columns(
            [
                pl.col("industry").map_elements(broad_sector, return_dtype=pl.Utf8).alias("broad_sector"),
                pl.col("industry").is_in(list(_FINANCIAL)).alias("is_financial"),
                pl.col("industry").is_in(list(_SPECIAL)).alias("is_special_category"),
                pl.lit("mops_operating_revenue").alias("industry_source"),
                pl.lit(TAXONOMY_VERSION).alias("taxonomy_version"),
            ]
        )
        .select(
            [
                "market",
                "company_code",
                "company_name",
                pl.col("year").cast(pl.Int32).alias("source_year"),
                pl.col("month").cast(pl.Int32).alias("source_month"),
                "source_ym",
                "effective_date",
                "raw_industry",
                "industry",
                "broad_sector",
                "is_financial",
                "is_special_category",
                "industry_source",
                "taxonomy_version",
            ]
        )
        .sort(["market", "company_code", "effective_date"])
    )
    return out


def fetch_industry_taxonomy_pit(con: Any) -> pl.DataFrame:
    """Return cached taxonomy if present, otherwise derive it from revenue rows."""
    try:
        return con.sql(
            """
            SELECT market, company_code, company_name, source_year, source_month,
                   source_ym, effective_date, raw_industry, industry, broad_sector,
                   is_financial, is_special_category, industry_source, taxonomy_version
            FROM industry_taxonomy_pit
            """
        ).pl()
    except Exception:
        return build_industry_taxonomy_pit(con)


def attach_industry_asof(panel: pl.DataFrame, con: Any, date_col: str = "date") -> pl.DataFrame:
    """Attach canonical point-in-time industry labels to a price/factor panel."""
    taxonomy = fetch_industry_taxonomy_pit(con)
    if taxonomy.is_empty():
        return panel.with_columns(
            [
                pl.lit(None, dtype=pl.Utf8).alias("industry"),
                pl.lit(None, dtype=pl.Utf8).alias("raw_industry"),
                pl.lit(None, dtype=pl.Utf8).alias("broad_sector"),
                pl.lit(None, dtype=pl.Boolean).alias("is_financial"),
                pl.lit(None, dtype=pl.Boolean).alias("is_special_category"),
            ]
        )

    tax = taxonomy.select(
        [
            "market",
            "company_code",
            "effective_date",
            "raw_industry",
            "industry",
            "broad_sector",
            "is_financial",
            "is_special_category",
            "industry_source",
            "taxonomy_version",
        ]
    ).sort(["market", "company_code", "effective_date"])

    return (
        panel.sort(["market", "company_code", date_col])
        .join_asof(
            tax,
            left_on=date_col,
            right_on="effective_date",
            by=["market", "company_code"],
            strategy="backward",
            check_sortedness=False,
        )
        .sort(["company_code", date_col])
    )


__all__ = [
    "TAXONOMY_VERSION",
    "attach_industry_asof",
    "broad_sector",
    "build_industry_taxonomy_pit",
    "fetch_industry_taxonomy_pit",
    "normalize_industry_name",
]
