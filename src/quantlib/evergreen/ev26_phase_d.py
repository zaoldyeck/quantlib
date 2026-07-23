"""EV26 Phase D — 出場與濾網掃描(確認平原或找一致改進)。

Run: uv run --project . python -m quantlib.evergreen.ev26_phase_d
"""
from __future__ import annotations

from quantlib.evergreen.ev26_engine import EngineSpec, Lab, fmt


def main() -> None:
    lab = Lab()
    grid = [
        ("基準", EngineSpec()),
        ("trail 25", EngineSpec(trail=0.25)),
        ("trail 30", EngineSpec(trail=0.30)),
        ("trail 40", EngineSpec(trail=0.40)),
        ("lts 20", EngineSpec(lts=20)),
        ("lts 40", EngineSpec(lts=40)),
        ("lts None", EngineSpec(lts=None)),
        ("time_stop 120", EngineSpec(time_stop=120)),
        ("recycle(1.0,0.5)", EngineSpec(profit_recycle=(1.0, 0.5))),
        ("h120 0.6", EngineSpec(h120_gate=0.6)),
        ("h120 0.8", EngineSpec(h120_gate=0.8)),
        ("mom_gate", EngineSpec(mom_gate=True)),
        ("池籍 3 月", EngineSpec(pool_months=3)),
        ("池籍 5 月", EngineSpec(pool_months=5)),
    ]
    for rn in ["v2", "v1"]:
        print(f"===== registry_{rn} =====")
        for name, spec in grid:
            print(fmt(name, lab.run(rn, spec)))


if __name__ == "__main__":
    main()
