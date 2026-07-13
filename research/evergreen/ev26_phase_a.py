"""EV26 Phase A — regime guard 家族:停新倉/清倉 × MA60/MA120。

Run: uv run --project research python -m research.evergreen.ev26_phase_a
"""
from __future__ import annotations

from research.evergreen.ev26_engine import EngineSpec, Lab, fmt


def main() -> None:
    lab = Lab()
    grid = [("基準(無 guard)", EngineSpec())]
    for regime in ["halt_new", "flatten"]:
        for ma in [60, 120]:
            grid.append((f"{regime}×MA{ma}",
                         EngineSpec(regime=regime, regime_ma=ma)))
    for rn in ["v2", "v1"]:
        print(f"===== registry_{rn} =====")
        for name, spec in grid:
            print(fmt(name, lab.run(rn, spec)))


if __name__ == "__main__":
    main()
