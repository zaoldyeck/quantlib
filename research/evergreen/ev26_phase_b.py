"""EV26 Phase B — 倉位/席位:分散度 × 權重結構。

Run: uv run --project research python -m research.evergreen.ev26_phase_b
"""
from __future__ import annotations

from research.evergreen.ev26_engine import EngineSpec, Lab, fmt


def main() -> None:
    lab = Lab()
    grid = [
        ("基準 5席mn2 conv(.10-.30)", EngineSpec()),
        ("7席 mn2", EngineSpec(n_slots=7)),
        ("7席 mn3", EngineSpec(n_slots=7, max_new=3)),
        ("10席 mn3", EngineSpec(n_slots=10, max_new=3)),
        ("5席 等權", EngineSpec(weight_mode="equal")),
        ("7席 等權", EngineSpec(n_slots=7, weight_mode="equal")),
        ("5席 clip .10-.25", EngineSpec(weight_clip=(0.10, 0.25))),
        ("5席 clip .08-.20", EngineSpec(weight_clip=(0.08, 0.20))),
        ("6席 mn2", EngineSpec(n_slots=6)),
        ("4席 mn2", EngineSpec(n_slots=4)),
    ]
    for rn in ["v2", "v1"]:
        print(f"===== registry_{rn} =====")
        for name, spec in grid:
            print(fmt(name, lab.run(rn, spec)))


if __name__ == "__main__":
    main()
