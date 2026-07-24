"""一次性消類:src/quantlib 內 `Path(__file__).resolve().parents[N]` 自算根錨全清。

rename(research→src/quantlib)讓檔案深度 +1,自算根錨全數錯位(data_calendar 休市日曆、
fubon .env 皆此類)。修法分三型:
- **死變數**(定義後檔內零引用;Phase 1b 消費端已 paths 化的殘留)→ 刪定義行。
- **sys.path hack**(ROOT=parents[n] + `if str(ROOT) not in sys.path: insert`)→ 三行刪
  (editable install 後不需要;且 rename 後指錯層)。
- **活根錨**(引用 >0 且語義=repo 根)→ 改 `paths.REPO`(必要時補 import)。
白名單:套件內相對資源(HERE.parents[0]/"registry" 類,不以 repo 為錨,rename 免疫)。

Run: uv run --project . python -m quantlib.tools.fix_parents_anchors [--apply]
(預設 dry-run 印計畫;--apply 落盤。)
"""
from __future__ import annotations

import re
import sys
from pathlib import Path

from quantlib import paths

_DEF = re.compile(r"^(\s*)(_?[A-Za-z_][A-Za-z0-9_]*)\s*=\s*Path\(__file__\)\.resolve\(\)\.parents\[(\d+)\]\s*(#.*)?$")
_IMPORT_PATHS = re.compile(r"^from quantlib import .*\bpaths\b|^from quantlib import paths")


def process(fp: Path, apply: bool) -> list[str]:
    text = fp.read_text(encoding="utf-8")
    lines = text.splitlines()
    actions: list[str] = []
    out: list[str] = []
    has_paths_import = any(_IMPORT_PATHS.match(l) for l in lines)
    need_paths_import = False
    i = 0
    while i < len(lines):
        line = lines[i]
        m = _DEF.match(line)
        if not m:
            out.append(line)
            i += 1
            continue
        indent, var = m.group(1), m.group(2)
        # 檔內其他行對該變數的引用(word boundary,排除本定義行)
        refs = [l for j, l in enumerate(lines) if j != i and re.search(rf"\b{re.escape(var)}\b", l)]
        # sys.path hack 型:下兩行是 if str(VAR) not in sys.path: insert
        nxt = lines[i + 1] if i + 1 < len(lines) else ""
        nxt2 = lines[i + 2] if i + 2 < len(lines) else ""
        if f"str({var}) not in sys.path" in nxt and "sys.path.insert" in nxt2:
            other = [l for l in refs if "sys.path" not in l]
            if not other:
                actions.append(f"{fp}: 刪 sys.path hack({var},3 行)")
                i += 3
                continue
        if not refs:
            actions.append(f"{fp}: 刪死變數 {var}")
            i += 1
            continue
        # 活根錨 → paths.REPO
        actions.append(f"{fp}: {var} = paths.REPO(引用 {len(refs)} 處:{refs[0].strip()[:60]})")
        out.append(f"{indent}{var} = paths.REPO")
        if not has_paths_import:
            need_paths_import = True
        i += 1
    if apply and actions:
        if need_paths_import:
            # 插在第一個 import 區塊之後
            for k, l in enumerate(out):
                if l.startswith("import ") or l.startswith("from "):
                    last_import = k
            out.insert(last_import + 1, "from quantlib import paths")
        fp.write_text("\n".join(out) + "\n", encoding="utf-8")
    return actions


def main() -> None:
    apply = "--apply" in sys.argv
    root = paths.REPO / "src" / "quantlib"
    all_actions: list[str] = []
    for fp in sorted(root.rglob("*.py")):
        if "tests" in fp.parts or fp.name == "paths.py" or fp.name == Path(__file__).name:
            continue
        if "Path(__file__).resolve().parents[" not in fp.read_text(encoding="utf-8"):
            continue
        all_actions += process(fp, apply)
    for a in all_actions:
        print(("[APPLY] " if apply else "[DRY] ") + a)
    print(f"\n共 {len(all_actions)} 項{'(已落盤)' if apply else '(dry-run;--apply 落盤)'}")


if __name__ == "__main__":
    main()
