"""apex trial 帳本 — 反過擬合的地基:每筆 trial 必記錄,equity curve 必保存。

單一寫入者原則:只有主 loop 呼叫 log_trial;並行 agent 回傳結果由主 loop 記錄。
trials.jsonl 一行一筆;curves/<trial_id>.parquet 存日 NAV(DSR/PBO 原料)。
"""
from __future__ import annotations

import json
import os
from datetime import datetime

import polars as pl

LEDGER_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "ledger")
TRIALS_PATH = os.path.join(LEDGER_DIR, "trials.jsonl")
CURVES_DIR = os.path.join(LEDGER_DIR, "curves")


def trial_count() -> int:
    if not os.path.exists(TRIALS_PATH):
        return 0
    with open(TRIALS_PATH, encoding="utf-8") as f:
        return sum(1 for line in f if line.strip())


def log_trial(
    *,
    family: str,
    name: str,
    hypothesis: str,
    config: dict,
    window: str,
    metrics: dict,
    batch: str,
    notes: str = "",
    curve: pl.DataFrame | None = None,
) -> str:
    """記錄一筆 trial,回傳 trial_id(T0001…)。curve 需含 date/nav 欄。"""
    os.makedirs(LEDGER_DIR, exist_ok=True)
    trial_id = f"T{trial_count() + 1:04d}"
    record = {
        "trial_id": trial_id,
        "ts": datetime.now().isoformat(timespec="seconds"),
        "batch": batch,
        "family": family,
        "name": name,
        "hypothesis": hypothesis,
        "window": window,
        "config": config,
        "metrics": _jsonable(metrics),
        "notes": notes,
    }
    with open(TRIALS_PATH, "a", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=False) + "\n")
    if curve is not None:
        os.makedirs(CURVES_DIR, exist_ok=True)
        curve.select(["date", "nav"]).write_parquet(os.path.join(CURVES_DIR, f"{trial_id}.parquet"))
    return trial_id


def all_trials() -> pl.DataFrame:
    """讀回全部 trial 記錄。config/metrics 混型(str 或 dict)一律轉 JSON 字串,
    避免 read_ndjson 的跨列 schema 統一失敗。"""
    if not os.path.exists(TRIALS_PATH):
        return pl.DataFrame()
    rows = []
    with open(TRIALS_PATH, encoding="utf-8") as f:
        for line in f:
            if not line.strip():
                continue
            r = json.loads(line)
            r["config"] = json.dumps(r.get("config"), ensure_ascii=False)
            r["metrics"] = json.dumps(r.get("metrics"), ensure_ascii=False)
            rows.append(r)
    return pl.DataFrame(rows)


def load_curve(trial_id: str) -> pl.DataFrame:
    return pl.read_parquet(os.path.join(CURVES_DIR, f"{trial_id}.parquet"))


def _jsonable(d: dict) -> dict:
    out = {}
    for k, v in d.items():
        if isinstance(v, float) and (v != v or v in (float("inf"), float("-inf"))):
            out[k] = None
        elif hasattr(v, "item"):  # numpy scalar
            out[k] = v.item()
        else:
            out[k] = v
    return out
