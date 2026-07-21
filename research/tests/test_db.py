from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]


def test_cache_connect_is_read_only_parallel() -> None:
    """Default research.db.connect() must not take DuckDB's single-writer lock."""
    script = (
        "import os, sys; "
        f"sys.path.insert(0, {str((ROOT / 'research').resolve())!r}); "
        "from db import RAW_QUARTERLY_PARQUET, connect; "
        "con = connect(); "
        "daily_count = con.sql('SELECT COUNT(*) FROM daily_quote').fetchone()[0]; "
        "raw_count = con.sql('SELECT COUNT(*) FROM raw_quarterly').fetchone()[0] "
        "if os.path.exists(RAW_QUARTERLY_PARQUET) else 0; "
        "print(f'{daily_count},{raw_count}'); "
        "con.close()"
    )
    env = {**os.environ, "PYTHONPATH": str(ROOT / "research")}
    procs = [
        subprocess.Popen(
            [sys.executable, "-c", script],
            cwd=ROOT,
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        for _ in range(2)
    ]
    results = [p.communicate(timeout=30) for p in procs]
    codes = [p.returncode for p in procs]

    assert codes == [0, 0], results
    counts = [tuple(map(int, stdout.strip().split(","))) for stdout, _ in results]
    assert counts[0][0] > 0
    assert counts[0] == counts[1]
