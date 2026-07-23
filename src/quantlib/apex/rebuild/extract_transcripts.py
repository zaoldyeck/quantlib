"""從 Claude Code 對話 transcript 逐字復原 heredoc 實驗代碼(零 LLM)。

apex/EV 戰役中後期實驗以 Bash heredoc 執行未落檔,但 transcript(.jsonl)
完整記錄每個 tool_use 的 command——本工具抽取所有含實驗痕跡的 heredoc
python 區塊與 Write 內容,按 trials.jsonl 的 trial name 歸檔:

    src/quantlib/apex/rebuild/recovered/{seq:03d}_{主要batch}.py   逐字原版代碼
    src/quantlib/apex/rebuild/recovered/index.json                 trial → 檔案對照
    stdout                                                     coverage 統計

Run: uv run --project . python -m quantlib.apex.rebuild.extract_transcripts
依賴 cache: 否
"""
from __future__ import annotations

import json
import re
from collections import defaultdict
from pathlib import Path

TRANSCRIPT_DIR = Path.home() / ".claude/projects/-Users-zaoldyeck-Documents-scala-quantlib"
OUT = Path("src/quantlib/apex/rebuild/recovered")
TRIALS = Path("src/quantlib/apex/ledger/trials.jsonl")

MARKERS = ("log_trial", "simulate(", "build_features", "harvest", "membership")
HEREDOC = re.compile(r"<<'(EOF|PY|PYEOF|SCRIPT)'\n(.*?)\n\1(?:\n|$)", re.S)


def iter_commands(path: Path):
    """yield (timestamp, tool_name, code_text) — Bash heredoc 內文與 Write 內容。"""
    with open(path, errors="ignore") as fh:
        for line in fh:
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                continue
            msg = rec.get("message") or {}
            for blk in (msg.get("content") or []):
                if not (isinstance(blk, dict) and blk.get("type") == "tool_use"):
                    continue
                ts = rec.get("timestamp", "?")
                inp = blk.get("input") or {}
                if blk.get("name") == "Bash":
                    cmd = inp.get("command", "")
                    for m in HEREDOC.finditer(cmd):
                        yield ts, "Bash", m.group(2)
                elif blk.get("name") == "Write":
                    yield ts, f"Write:{inp.get('file_path', '?')}", inp.get("content", "")


def main() -> None:
    names = []
    for l in open(TRIALS):
        n = json.loads(l).get("name")
        if n and n not in names:
            names.append(n)
    name_pats = {n: re.compile(re.escape(n)) for n in names}
    # 動態命名(f"r27_lts{v}")比對:batch 前綴出現 + 實驗痕跡即歸該 batch
    batch_of = {n: re.match(r"[^\W_]+", n).group(0) for n in names}

    def match_names(code: str) -> list[str]:
        # 三層聯集:逐字 trial name ∪ batch 前綴(動態命名)∪ 值域特徵字串
        hit = {n for n, p in name_pats.items() if p.search(code)}
        pref = {b for b in set(batch_of.values())
                if len(b) >= 3 and (f'"{b}_' in code or f"'{b}_" in code
                                    or f"{b}_{{" in code or f'f"{b}' in code)}
        hit |= {n for n, b in batch_of.items() if b in pref}
        feats = ("全跨度14", "正2全史同窗", "現代era", "b11ef", "b12ab",
                 "modern_r0", "oos_r0", "holdout_v3", "matchwin", "fullspan_v3",
                 "v02_", "v01_")
        hit_f = [f for f in feats if f in code]
        hit |= {n for n in names
                if any(n.startswith(f.rstrip("_")) for f in hit_f)}
        return sorted(hit)

    OUT.mkdir(parents=True, exist_ok=True)
    index: dict[str, list[str]] = defaultdict(list)
    seq = 0
    seen_code: set[str] = set()

    for tp in sorted(TRANSCRIPT_DIR.glob("*.jsonl")):
        for ts, src, code in iter_commands(tp):
            if not any(k in code for k in MARKERS):
                continue
            if code in seen_code:
                continue
            seen_code.add(code)
            hit = match_names(code)
            if not hit:
                continue
            seq += 1
            main_batch = re.match(r"[^\W_]+", hit[0]).group(0)
            f = OUT / f"{seq:03d}_{main_batch}.py"
            header = (f'"""transcript 逐字復原(零改動)。\n\n'
                      f"來源:{tp.name} @ {ts}(工具 {src})\n"
                      f"涵蓋 trials({len(hit)}):{', '.join(hit[:20])}"
                      f"{' …' if len(hit) > 20 else ''}\n"
                      f'"""\n')
            f.write_text(header + code + "\n")
            for n in hit:
                index[n].append(f.name)

    (OUT / "index.json").write_text(
        json.dumps(index, ensure_ascii=False, indent=1, sort_keys=True))

    covered = set(index)
    missing_before = json.load(open("src/quantlib/apex/rebuild/packs.json"))
    target = {t["name"] for p in missing_before for t in p["trials"]}
    print(f"復原代碼段:{seq} 檔;涵蓋 trial:{len(covered)}/{len(names)}(全部)")
    print(f"缺檔 trial 覆蓋:{len(target & covered)}/{len(target)}")
    still = sorted(target - covered)
    print(f"仍無代碼:{len(still)}")
    if still:
        by_batch = defaultdict(int)
        for n in still:
            by_batch[re.match(r"[^\W_]+", n).group(0)] += 1
        for b, c in sorted(by_batch.items()):
            print(f"  {b:14s} {c}")


if __name__ == "__main__":
    main()
