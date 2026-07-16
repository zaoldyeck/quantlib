"""哲學 JSON → 標記提示詞 inline 文字(組裝端,考古重建自 phil_inline 結構)。

當年 EV27 的組裝以 heredoc 執行未落檔;本檔逆向重建,並以「原版
ev27_philosophy.json 重組 → 必須逐字重現 ev27_phil_inline.txt」驗證正確性。
EV45(Fable 蒸餾版)沿用同一組裝器,保證兩版格式逐字同構。

Run: uv run --project research python -m research.evergreen.phil_assemble <in.json> <out.txt>
     (無參數 = 自驗模式:重組 ev27 版並 diff)
依賴 cache: 否
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

EG = Path("research/evergreen/data")


def assemble(d: dict) -> str:
    parts = ["【判斷哲學(你唯一的判斷框架)】", d["philosophy"], ""]
    parts.append("【十道判別(真身 ⇄ 偽形,逐檔成對檢查)】")
    for i, x in enumerate(d["discriminations"], 1):
        parts.append(f"{i}. {x['name']}|真身:{x['true_form']}|偽形:{x['false_form']}")
    parts += ["", "【催化判讀】", d["catalyst_reading"], ""]
    parts.append("【訊號類型(前兆⇄排除)】")
    for x in d["signal_types"]:
        parts.append(f"◆ {x['name']}:{x['definition']}|前兆:{';'.join(x['precursors'])}"
                     f"|排除:{';'.join(x['exclusions'])}")
    parts += ["", "【標記流程】" + " → ".join(d["checklist"]), ""]
    parts.append("【一票否決】" + ";".join(d["anti_patterns"]))
    parts += ["", "【conviction 給分】", d["conviction_rubric"]]
    return "\n".join(parts)


def main() -> None:
    if len(sys.argv) == 3:
        d = json.load(open(sys.argv[1]))
        Path(sys.argv[2]).write_text(assemble(d))
        print(f"組裝 → {sys.argv[2]}({len(assemble(d)):,} 字)")
        return
    # 自驗:原版 JSON 重組必須逐字重現 phil_inline
    d = json.load(open(EG / "ev27_philosophy.json"))
    mine = assemble(d)
    ref = (EG / "ev27_phil_inline.txt").read_text()
    if mine == ref:
        print("✓ 組裝器驗證通過:逐字重現 ev27_phil_inline.txt")
    else:
        import difflib
        print(f"✗ 不一致(重組 {len(mine)} vs 原版 {len(ref)});前 12 行 diff:")
        for l in list(difflib.unified_diff(ref.splitlines(), mine.splitlines(),
                                           lineterm=""))[:12]:
            print(l)
        sys.exit(1)


if __name__ == "__main__":
    main()
