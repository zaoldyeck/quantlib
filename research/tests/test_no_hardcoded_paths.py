"""防復發法條:產物位置不得再寫成字面值,一律從 `research.paths` 取。

背景(2026-07-22 結構稽核):`cache.duckdb` 硬編在 **75 個檔**、
`strat_lab/results` 在 **104 個檔**。路徑寫成字面值時,「搬一次目錄」等於
「改 180 個地方」,於是沒人敢搬,於是結構永遠爛在原地——這條規則就是要讓
那個狀態不可能再發生。

判定方式刻意用 **AST**,只看「程式碼裡真的拿來當路徑用的字串」:
- docstring 與註解不算(文件本來就該寫得出具體路徑)
- `research/paths.py` 自己不算(它就是定義的地方)
- 逐字封存的歷史復原碼不算(`apex/rebuild/`、`apex/experiments/` 是
  transcript 原樣搶救回來的實驗紀錄,改動它們等於竄改研究史)

Run: uv run --project research python -m pytest research/tests/test_no_hardcoded_paths.py
"""
from __future__ import annotations

import ast
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]

#: 這些片段一旦出現在**程式碼字串**裡,就是把產物位置寫死了
FORBIDDEN = (
    "cache.duckdb", "cache_s_slim.duckdb",
    "var/out", "var/cache", "var/state", "var/reports", "var/log",
    "strat_lab/results", "data/intraday",
    "research/out", "research/state", "research/data",
    "tri/reports", "apex/reports",
)

#: 定義處與逐字封存的歷史紀錄
EXEMPT_FILES = {"research/paths.py",
                "research/tests/test_no_hardcoded_paths.py"}  # 定義處 + 法條本身
EXEMPT_DIRS = ("research/apex/rebuild/", "research/apex/experiments/")


def _code_strings(tree: ast.AST) -> list[tuple[int, str]]:
    """回傳所有**非 docstring** 的字串常數 (行號, 值)。"""
    docstrings = set()
    for node in ast.walk(tree):
        if isinstance(node, (ast.Module, ast.ClassDef, ast.FunctionDef,
                             ast.AsyncFunctionDef)):
            body = getattr(node, "body", None)
            if body and isinstance(body[0], ast.Expr) and \
                    isinstance(body[0].value, ast.Constant) and \
                    isinstance(body[0].value.value, str):
                docstrings.add(id(body[0].value))
    return [(n.lineno, n.value) for n in ast.walk(tree)
            if isinstance(n, ast.Constant) and isinstance(n.value, str)
            and id(n) not in docstrings]


def _offenders() -> list[str]:
    out: list[str] = []
    for p in sorted((REPO / "research").rglob("*.py")):
        rel = p.relative_to(REPO).as_posix()
        if ".venv" in rel or rel in EXEMPT_FILES or rel.startswith(EXEMPT_DIRS):
            continue
        try:
            tree = ast.parse(p.read_text(encoding="utf-8"))
        except (SyntaxError, UnicodeDecodeError, OSError):
            continue
        for lineno, s in _code_strings(tree):
            for bad in FORBIDDEN:
                if bad in s:
                    out.append(f"{rel}:{lineno} 硬編路徑 {s!r}(應改用 research.paths)")
    return out


def test_no_hardcoded_product_paths() -> None:
    bad = _offenders()
    assert not bad, (
        f"發現 {len(bad)} 處硬編產物路徑——請改從 `research.paths` 取:\n  "
        + "\n  ".join(bad[:20]))


def test_paths_module_is_repo_anchored() -> None:
    """所有路徑必須以 repo 根為錨。相對路徑會隨 cwd 漂移,那在本專案出過災難級
    事故(2026-07-22:advisors 由別的 cwd 啟動時靜默讀到空,全部持股被判為外人)。"""
    from research import paths
    for name in ("RAW", "VAR", "CACHE_DB", "OUT", "REPORTS", "LOG", "STATE",
                 "RECORDS", "RAW_INTRADAY", "STATE_LIVE", "OUT_EXECUTIONS"):
        v = getattr(paths, name)
        assert v.is_absolute(), f"paths.{name} 不是絕對路徑:{v}"
        assert paths.REPO in v.parents or v == paths.REPO, f"paths.{name} 不在 repo 內"


def test_three_lifecycles_do_not_overlap() -> None:
    """原始封存 / 可重生產物 / 版控紀錄三個根不得互相包含——一旦重疊,
    `.gitignore` 的一行規則就會誤傷,或產物混進版控。"""
    from research import paths
    roots = [paths.RAW, paths.VAR, paths.RECORDS]
    for i, a in enumerate(roots):
        for b in roots[i + 1:]:
            assert a not in b.parents and b not in a.parents, f"{a} 與 {b} 重疊"


def test_paths_module_is_never_shadowed() -> None:
    """任何檔案 import 了 `paths` 之後,函式內不得再有同名區域變數。

    2026-07-22 實例:`s_advisor` 內原本就有 `paths = load_paths(...)`(價格路徑),
    模組級加入 `from research import paths` 後,Python 在編譯期就把整個函式裡的
    `paths` 判為區域變數 → 函式開頭引用 `paths.RECORDS` 直接 `UnboundLocalError`。

    **這種 bug 測試套件抓不到**(那條路徑當時沒有覆蓋),是在部署到 VM 後試跑才
    炸出來的——所以要有一條靜態法條把它擋在門外。
    """
    offenders = []
    for p in sorted((REPO / "research").rglob("*.py")):
        rel = p.relative_to(REPO).as_posix()
        if ".venv" in rel or rel in EXEMPT_FILES or rel.startswith(EXEMPT_DIRS):
            continue
        try:
            tree = ast.parse(p.read_text(encoding="utf-8"))
        except (SyntaxError, UnicodeDecodeError, OSError):
            continue
        if not any(isinstance(n, ast.ImportFrom) and n.module == "research"
                   and any(a.name == "paths" for a in n.names) for n in tree.body):
            continue
        for fn in ast.walk(tree):
            if not isinstance(fn, (ast.FunctionDef, ast.AsyncFunctionDef)):
                continue
            for n in ast.walk(fn):
                if isinstance(n, ast.Name) and isinstance(n.ctx, ast.Store) \
                        and n.id == "paths":
                    offenders.append(f"{rel}:{n.lineno} 函式 {fn.name}() 內遮蔽了 paths")
                    break
    assert not offenders, "paths 被區域變數遮蔽(會在執行期 UnboundLocalError):\n  " \
        + "\n  ".join(offenders)
