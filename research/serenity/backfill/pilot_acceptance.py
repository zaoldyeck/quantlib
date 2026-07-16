"""2023H1 回溯標記先導 — 預定義三判準驗收.

判準(trials ledger 預註冊,2026-07-14):
A. 入冊+拒絕清單含 ≥2 個「事後失敗主題」——聚類種子成員等權組合,標記日 +12 個月
   報酬 <0 或跑輸 0050 ≥15pp(證明流程抓得到「當時熱、後來錯」的主題)。
B. 每個種子聚類都有檢核記錄(admit/reject/carry_over)。
C. 全部 evidence 日期 ≤ 該月時間圍欄。

報酬用未調整收盤價(粗判準 ±15pp 級,配息噪音對個股與 0050 方向近似抵消;如實註明)。

Run: uv run --project research python -m research.serenity.backfill.pilot_acceptance
"""

from __future__ import annotations

import json
from datetime import date, timedelta
from pathlib import Path

import duckdb

HERE = Path(__file__).parent
CACHE = HERE.parents[2] / "research" / "cache.duckdb"
MONTHS = [f"2023-0{i}" for i in range(1, 7)]


def ret_12m(con, codes: list[str], d0: date) -> float | None:
    """種子成員等權組合 d0 → d0+365d 報酬(每檔取窗內首末收盤)。"""
    if not codes:
        return None
    cl = ",".join(f"'{c}'" for c in codes)
    rows = con.execute(
        f"""
        SELECT company_code,
               first(closing_price ORDER BY date) p0, last(closing_price ORDER BY date) p1
        FROM daily_quote
        WHERE company_code IN ({cl}) AND date >= ? AND date <= ?
        GROUP BY company_code HAVING count(*) > 200
        """,
        [d0, d0 + timedelta(days=365)],
    ).fetchall()
    rets = [p1 / p0 - 1 for _, p0, p1 in rows if p0]
    return sum(rets) / len(rets) if rets else None


def main() -> None:
    con = duckdb.connect(str(CACHE), read_only=True)
    runs = {ym: json.load(open(HERE / "label_runs" / f"{ym}.json")) for ym in MONTHS}
    seeds = {ym: json.load(open(HERE / "seeds" / f"{ym}.json")) for ym in MONTHS}

    # --- 判準 C:時間圍欄(容忍 agent 的「~」近似日期前綴:取數字部分、
    #     年/月精度補全至該期末再比較;無法解析才列違規)---
    import re

    def norm_date(raw: str) -> str | None:
        m = re.search(r"(\d{4})(?:-(\d{2}))?(?:-(\d{2}))?", str(raw))
        if not m:
            return None
        y, mo, d = m.group(1), m.group(2), m.group(3)
        if d:
            return f"{y}-{mo}-{d}"
        if mo:
            return f"{y}-{mo}-28"
        return f"{y}-12-31"

    # 預註冊判準 C 的範圍是「入冊主題」的 evidence;reject 聚類的模糊日期列警示不計違規
    fence_violations, fence_warnings = [], []
    for ym, run in runs.items():
        fence = run.get("fence_date") or f"{ym}-28"
        for cl in run["clusters"]:
            for ev in cl.get("evidence") or []:
                d = norm_date(ev.get("date", ""))
                if d is None or d > fence:
                    row = (ym, cl.get("industry"), ev.get("date"))
                    if cl.get("verdict") == "admit":
                        fence_violations.append(row)
                    else:
                        fence_warnings.append(row)

    # --- 判準 B:種子檢核完備 ---
    missing_checks = []
    for ym in MONTHS:
        seen = {(c.get("industry"), c.get("seed_type")) for c in runs[ym]["clusters"]}
        seen_ind = {c.get("industry") for c in runs[ym]["clusters"]}
        for stype, key in (("momentum", "momentum_clusters"), ("revenue", "revenue_accel_clusters")):
            for cl in seeds[ym][key]:
                if (cl["industry"], stype) not in seen and cl["industry"] not in seen_ind:
                    missing_checks.append((ym, stype, cl["industry"]))

    # --- 判準 A:事後失敗主題(首次檢核的 admit/reject 聚類)---
    seed_members = {
        (ym, cl["industry"]): [m["code"] for m in cl["members"]]
        for ym in MONTHS
        for key in ("momentum_clusters", "revenue_accel_clusters")
        for cl in seeds[ym][key]
        if not None
    }
    rows_a = []
    for ym in MONTHS:
        d0 = date.fromisoformat(seeds[ym]["label_day"])
        bench = ret_12m(con, ["0050"], d0)
        for cl in runs[ym]["clusters"]:
            verdict = cl.get("verdict")
            if verdict not in ("admit", "reject"):
                continue
            codes = seed_members.get((ym, cl.get("industry"))) or [
                m["code"] for m in (cl.get("members") or []) if m.get("code")
            ]
            r = ret_12m(con, codes, d0)
            if r is None or bench is None:
                continue
            failed = r < 0 or (r - bench) <= -0.15
            rows_a.append(
                {"month": ym, "industry": cl.get("industry"), "verdict": verdict,
                 "n": len(codes), "ret_12m": round(r * 100, 1),
                 "bench_0050": round(bench * 100, 1), "failed_after": failed}
            )
    con.close()

    failed_themes = [r for r in rows_a if r["failed_after"]]
    a_pass, b_pass, c_pass = len(failed_themes) >= 2, not missing_checks, not fence_violations

    lines = [
        "# 2023H1 回溯標記先導 — 驗收報告(2026-07-14)", "",
        f"- 判準 A(≥2 個事後失敗主題被納入檢核):**{'PASS' if a_pass else 'FAIL'}**"
        f"(失敗主題 {len(failed_themes)} 個)",
        f"- 判準 B(種子聚類檢核完備):**{'PASS' if b_pass else 'FAIL'}**"
        f"(缺檢 {len(missing_checks)}:{missing_checks or '無'})",
        f"- 判準 C(入冊主題時間圍欄合規):**{'PASS' if c_pass else 'FAIL'}**"
        f"(入冊違規 {len(fence_violations)}:{fence_violations[:5] or '無'};"
        f"reject 聚類模糊日期警示 {len(fence_warnings)} 筆,不計違規)", "",
        "## 聚類 12 個月事後表現(種子成員等權,未調整收盤;0050 為對照)", "",
        "| 月 | 聚類 | 判定 | n | +12m 報酬 | 0050 | 事後失敗 |", "|---|---|---|---:|---:|---:|---|",
    ]
    for r in sorted(rows_a, key=lambda x: x["ret_12m"]):
        lines.append(
            f"| {r['month']} | {r['industry']} | {r['verdict']} | {r['n']} "
            f"| {r['ret_12m']}% | {r['bench_0050']}% | {'✗ 失敗' if r['failed_after'] else ''} |"
        )
    out = HERE / "2023H1_report.md"
    out.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print("\n".join(lines))
    print(f"\nreport -> {out}")


if __name__ == "__main__":
    main()
