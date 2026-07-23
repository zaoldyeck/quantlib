from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[4]
sys.path.insert(0, str(ROOT / "src" / "quantlib" / "strat_lab"))

from iter_68_position_level_bridge import load_pick_targets


def test_load_pick_targets_preserves_leading_zero_company_codes(tmp_path: Path) -> None:
    path = tmp_path / "picks.csv"
    path.write_text("rebal_d,company_code,weight\n2020-01-02,0050,1.0\n", encoding="utf-8")

    targets = load_pick_targets(path)

    assert targets[next(iter(targets))] == {"0050": 1.0}
