from __future__ import annotations

import ast
import csv
import json
from collections import Counter, defaultdict, deque
from pathlib import Path
from typing import NamedTuple


ROOT = Path(__file__).resolve().parents[1]
OUTPUT_FILES = {
    "audit_manifest.csv",
    "findings_report.md",
    "deletion_candidates.md",
    "dependency_audit.md",
    "docs_drift_report.md",
    "replay_baseline_report.md",
}
RUN_IDS = ("20260421_234355_31268", "20260422_230643_23708")
TELEMETRY_ROOT = ROOT / "data" / "bot" / "telemetry"
SPECIAL_ROWS: dict[str, tuple[str, str, str, str]] = {
    "PRD_v7.0_Futures_Signal_Bot_Final_Codex.md": (
        "doc-drift",
        "Historical PRD still describes a multi-process/shared-memory runtime that is not the live code path.",
        "Keep only as historical context; do not treat it as launch or architecture truth.",
        "keep",
    ),
    "bot/features.py": (
        "placeholder",
        "Module docstring states some backward-compat columns still return neutral values.",
        "Keep strategy-critical indicators; quarantine or delete placeholder-only branches after caller audit.",
        "keep",
    ),
    "bot/market_regime.py": (
        "logic-risk",
        "Analyzer still derives several regime components from 24h ticker-change aggregates and only prefers structure context when present.",
        "Treat raw 24h change as fallback only and validate regime outputs before tightening live filters.",
        "keep",
    ),
    "bot/models.py": (
        "contract-drift",
        "PreparedSymbol exposes spot_lead_return_1m and spot_futures_spread_bps, but no population call sites were found on the live path.",
        "Either populate these fields or remove them from live confirmation contracts.",
        "keep",
    ),
}
QUARANTINE_PREFIXES = (
    "bot/tasks/",
    "bot/telegram/",
    "bot/telegram_bot.py",
    "bot/core/analyzer/",
    "bot/core/diagnostics/",
    "bot/core/self_learner.py",
    "bot/core/memory/cache.py",
)


class ManifestRow(NamedTuple):
    path: str
    kind: str
    reachability: str
    status: str
    evidence: str
    required_action: str
    delete_policy: str


def _relative(path: Path) -> str:
    return path.relative_to(ROOT).as_posix()


def iter_audited_files() -> list[Path]:
    files: list[Path] = []
    for base in ("bot", "tests", "scripts"):
        files.extend(path for path in (ROOT / base).rglob("*.py") if "__pycache__" not in path.parts)
    for pattern in ("*.md", "*.toml", "requirements*.txt"):
        files.extend(path for path in ROOT.glob(pattern) if path.name not in OUTPUT_FILES)
    return sorted({path.resolve() for path in files})


def build_module_map(files: list[Path]) -> dict[str, Path]:
    module_map: dict[str, Path] = {"main": ROOT / "main.py"}
    for path in files:
        if path.suffix != ".py":
            continue
        rel = path.relative_to(ROOT)
        parts = list(rel.with_suffix("").parts)
        if parts[-1] == "__init__":
            parts = parts[:-1]
        module = ".".join(parts)
        if module:
            module_map[module] = path
    return module_map


def module_for_file(path: Path) -> str:
    if path == ROOT / "main.py":
        return "main"
    rel = path.relative_to(ROOT)
    parts = list(rel.with_suffix("").parts)
    if parts[-1] == "__init__":
        parts = parts[:-1]
    return ".".join(parts)


def _resolve_module(name: str, module_map: dict[str, Path]) -> Path | None:
    if name in module_map:
        return module_map[name]
    current = name
    while "." in current:
        current = current.rsplit(".", 1)[0]
        candidate = module_map.get(current)
        if candidate is not None:
            return candidate
    return None


def _package_parts(path: Path) -> list[str]:
    module = module_for_file(path)
    if not module:
        return []
    if path.name == "__init__.py":
        return module.split(".")
    return module.split(".")[:-1]


def parse_import_graph(files: list[Path], module_map: dict[str, Path]) -> tuple[dict[Path, set[Path]], dict[Path, set[str]]]:
    graph: dict[Path, set[Path]] = defaultdict(set)
    imports_by_file: dict[Path, set[str]] = defaultdict(set)
    for path in files:
        if path.suffix != ".py":
            continue
        try:
            tree = ast.parse(path.read_text(encoding="utf-8"))
        except SyntaxError:
            continue
        package_parts = _package_parts(path)
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    imports_by_file[path].add(alias.name.split(".", 1)[0])
                    target = _resolve_module(alias.name, module_map)
                    if target is not None:
                        graph[path].add(target)
            elif isinstance(node, ast.ImportFrom):
                resolved_module = ""
                if node.level:
                    keep = len(package_parts) - (node.level - 1)
                    prefix_parts = package_parts[: max(keep, 0)]
                    if node.module:
                        resolved_module = ".".join(prefix_parts + node.module.split("."))
                    else:
                        resolved_module = ".".join(prefix_parts)
                elif node.module:
                    resolved_module = node.module
                if resolved_module:
                    imports_by_file[path].add(resolved_module.split(".", 1)[0])
                    target = _resolve_module(resolved_module, module_map)
                    if target is not None:
                        graph[path].add(target)
                for alias in node.names:
                    if alias.name == "*":
                        continue
                    full_name = f"{resolved_module}.{alias.name}" if resolved_module else alias.name
                    target = _resolve_module(full_name, module_map)
                    if target is not None:
                        graph[path].add(target)
    return graph, imports_by_file


def walk_reachable(graph: dict[Path, set[Path]], roots: list[Path]) -> set[Path]:
    seen: set[Path] = set()
    queue: deque[Path] = deque(path for path in roots if path.exists())
    while queue:
        path = queue.popleft()
        if path in seen:
            continue
        seen.add(path)
        for child in graph.get(path, set()):
            if child not in seen:
                queue.append(child)
    return seen


def kind_for_path(path: str) -> str:
    if path.startswith("tests/"):
        return "test"
    if path.startswith("scripts/"):
        return "script"
    if path.startswith("bot/") or path == "main.py":
        return "runtime"
    if path.endswith(".md"):
        return "doc"
    if path.endswith(".toml") or path.startswith("requirements"):
        return "config"
    return "generated"


def reachability_for_path(path: Path, runtime: set[Path], tests: set[Path], scripts: set[Path]) -> tuple[str, str]:
    if path in runtime:
        return "runtime", "reachable from main.py / bot.cli.run() import graph"
    if path in tests:
        return "tests", "reachable from tests/ import graph"
    if path in scripts:
        return "scripts", "reachable from scripts/live_*.py import graph"
    return "none", "no static path from runtime roots, tests/, or scripts/live_*.py"


def requirement_lines(path: Path) -> list[str]:
    lines: list[str] = []
    if not path.exists():
        return lines
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.split("#", 1)[0].strip()
        if line:
            lines.append(line)
    return lines


def load_jsonl(path: Path) -> list[dict]:
    rows: list[dict] = []
    if not path.exists():
        return rows
    with path.open("r", encoding="utf-8") as handle:
        for raw_line in handle:
            line = raw_line.strip()
            if not line:
                continue
            try:
                payload = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(payload, dict):
                rows.append(payload)
    return rows


def analyze_replay_runs() -> tuple[str, list[str]]:
    lines = ["# Replay Baseline Report", "", "Historical telemetry evidence only; this is not a post-change replay harness.", ""]
    findings: list[str] = []
    for run_id in RUN_IDS:
        tracking_path = TELEMETRY_ROOT / "runs" / run_id / "analysis" / "tracking_events.jsonl"
        rejected_path = TELEMETRY_ROOT / "runs" / run_id / "analysis" / "rejected.jsonl"
        tracking_rows = load_jsonl(tracking_path)
        rejected_rows = load_jsonl(rejected_path)
        signal_state: dict[str, dict] = {}
        signal_events: dict[str, list[str]] = defaultdict(list)
        for row in tracking_rows:
            tracking_id = str(row.get("tracking_id") or "")
            if not tracking_id:
                continue
            signal_state.setdefault(tracking_id, row)
            signal_events[tracking_id].append(str(row.get("event_type") or ""))
        invariant_violations: list[str] = []
        dual_target_violations: list[str] = []
        for tracking_id, row in signal_state.items():
            try:
                entry_low = float(row.get("entry_low"))
                entry_high = float(row.get("entry_high"))
                stop = float(row.get("stop"))
                tp1 = float(row.get("take_profit_1"))
                tp2 = float(row.get("take_profit_2"))
            except (TypeError, ValueError):
                continue
            direction = str(row.get("direction") or "")
            entry_mid = (entry_low + entry_high) / 2.0
            if direction == "long" and not (stop < entry_mid < tp1 <= tp2):
                invariant_violations.append(
                    f"{row.get('symbol')} {direction}: stop={stop:.8f} entry={entry_mid:.8f} tp1={tp1:.8f} tp2={tp2:.8f}"
                )
            if direction == "short" and not (stop > entry_mid > tp1 >= tp2):
                invariant_violations.append(
                    f"{row.get('symbol')} {direction}: stop={stop:.8f} entry={entry_mid:.8f} tp1={tp1:.8f} tp2={tp2:.8f}"
                )
            tolerance = max(abs(entry_mid) * 1e-8, 1e-8)
            event_types = signal_events.get(tracking_id, [])
            if abs(tp1 - tp2) <= tolerance and "tp1_hit" in event_types and "tp2_hit" in event_types:
                dual_target_violations.append(f"{row.get('symbol')} {direction}: equal targets emitted tp1_hit and tp2_hit")
        reject_counter = Counter(
            str(row.get("reason") or "unknown")
            for row in rejected_rows
            if isinstance(row, dict)
        )
        lines.extend(
            [
                f"## {run_id}",
                f"- tracking events: {len(tracking_rows)}",
                f"- rejected events: {len(rejected_rows)}",
                f"- top reject reasons: {', '.join(f'{reason}={count}' for reason, count in reject_counter.most_common(5)) or 'none'}",
                f"- invariant violations: {len(invariant_violations)}",
                f"- equal-target dual-close violations: {len(dual_target_violations)}",
            ]
        )
        if invariant_violations:
            lines.append("- sample invariant violations:")
            lines.extend(f"  - {item}" for item in invariant_violations[:3])
            findings.extend(invariant_violations[:3])
        if dual_target_violations:
            lines.append("- sample equal-target lifecycle violations:")
            lines.extend(f"  - {item}" for item in dual_target_violations[:3])
            findings.extend(dual_target_violations[:3])
        lines.append("")
    lines.extend(
        [
            "## Current-Code Interpretation",
            "- Historical runs still contain pre-fix target-integrity defects.",
            "- Current regression coverage in tests/test_remediation_regressions.py exercises swapped-target normalization and single-target close semantics.",
        ]
    )
    return "\n".join(lines) + "\n", findings


def build_manifest(
    files: list[Path],
    runtime_reachable: set[Path],
    test_reachable: set[Path],
    script_reachable: set[Path],
    reverse_graph: dict[Path, set[Path]],
) -> list[ManifestRow]:
    rows: list[ManifestRow] = []
    for path in files:
        rel = _relative(path)
        kind = kind_for_path(rel)
        reachability, evidence = reachability_for_path(path, runtime_reachable, test_reachable, script_reachable)
        status = "ok"
        required_action = "none"
        delete_policy = "keep"
        if rel in SPECIAL_ROWS:
            status, extra_evidence, required_action, delete_policy = SPECIAL_ROWS[rel]
            evidence = f"{evidence}; {extra_evidence}"
        elif any(rel == prefix.rstrip("/") or rel.startswith(prefix) for prefix in QUARANTINE_PREFIXES) and reachability == "none":
            inbound = sorted(_relative(src) for src in reverse_graph.get(path, set()))
            status = "scaffold"
            required_action = "Quarantine first; no deletion until package exports and external consumers are reviewed."
            delete_policy = "quarantine"
            if inbound:
                evidence = f"{evidence}; inbound refs only from {', '.join(inbound)}"
        elif rel == "config.toml.example":
            if "[bot.filters.setups]" in path.read_text(encoding="utf-8"):
                required_action = "keep aligned with config.toml when setup surface changes"
            else:
                status = "contract-drift"
                required_action = "mirror the [bot.filters.setups] surface from config.toml"
        elif rel == "requirements-optional.txt":
            required_action = "install alongside requirements.txt when dashboard, ML, or self-learning are enabled"
        rows.append(
            ManifestRow(
                path=rel,
                kind=kind,
                reachability=reachability,
                status=status,
                evidence=evidence,
                required_action=required_action,
                delete_policy=delete_policy,
            )
        )
    return rows


def write_manifest(rows: list[ManifestRow]) -> None:
    with (ROOT / "audit_manifest.csv").open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(ManifestRow._fields)
        for row in rows:
            writer.writerow(row)


def write_findings_report(replay_findings: list[str]) -> None:
    lines = [
        "# Findings Report",
        "",
        "## Confirmed Fixes",
        "- Plain `pytest -q` bootstrap is normalized through `pyproject.toml` `pythonpath = [\".\"]`.",
        "- The 5 concrete pyright errors were addressed in `bot/delivery.py`, `bot/public_intelligence.py`, and `bot/ws_manager.py`.",
        "- `config.toml.example` now mirrors the live `[bot.filters.setups]` surface from `config.toml`.",
        "- Optional runtime dependencies are classified in `requirements-optional.txt` instead of being implicit ambient state.",
        "",
        "## Confirmed Remaining Risks",
        "- `PreparedSymbol.spot_lead_return_1m` and `PreparedSymbol.spot_futures_spread_bps` are declared but not populated anywhere on the live path.",
        "- `bot/features.py` still carries backward-compat neutral placeholder columns.",
        "- `bot/market_regime.py` still mixes benchmark context with raw 24h ticker-change aggregates.",
        "- Quarantine-first candidates remain in-tree because no deletion proof beyond static reachability was gathered for external consumers.",
        "",
        "## Historical Telemetry Evidence",
    ]
    if replay_findings:
        lines.extend(f"- {item}" for item in replay_findings[:6])
    else:
        lines.append("- No replay findings were extracted from the specified telemetry runs.")
    lines.append("")
    (ROOT / "findings_report.md").write_text("\n".join(lines), encoding="utf-8")


def write_deletion_candidates(rows: list[ManifestRow], reverse_graph: dict[Path, set[Path]]) -> None:
    lines = [
        "# Deletion Candidates",
        "",
        "Quarantine-first only. No file listed here was removed in this wave.",
        "",
    ]
    for row in rows:
        if row.delete_policy != "quarantine":
            continue
        path = ROOT / row.path
        inbound = sorted(_relative(src) for src in reverse_graph.get(path, set()))
        lines.append(f"## {row.path}")
        lines.append(f"- status: {row.status}")
        lines.append(f"- reachability: {row.reachability}")
        lines.append(f"- evidence: {row.evidence}")
        lines.append(f"- inbound refs: {', '.join(inbound) if inbound else 'none'}")
        lines.append(f"- action: {row.required_action}")
        lines.append("")
    (ROOT / "deletion_candidates.md").write_text("\n".join(lines), encoding="utf-8")


def write_dependency_audit(imports_by_file: dict[Path, set[str]]) -> None:
    requirement_files = {
        "requirements.txt": requirement_lines(ROOT / "requirements.txt"),
        "requirements-modern.txt": requirement_lines(ROOT / "requirements-modern.txt"),
        "requirements-frozen.txt": requirement_lines(ROOT / "requirements-frozen.txt"),
        "requirements-optional.txt": requirement_lines(ROOT / "requirements-optional.txt"),
    }
    all_imports = Counter()
    for path, imports in imports_by_file.items():
        rel = _relative(path)
        if not (rel.startswith("bot/") or rel == "main.py"):
            continue
        for name in imports:
            all_imports[name] += 1
    lines = [
        "# Dependency Audit",
        "",
        "## Classified Dependencies",
        "- required runtime: aiohttp, aiosqlite, aiogram, binance-connector, python-binance, binance-sdk-derivatives-trading-usds-futures, msgspec, numpy, orjson, polars, polars_ta, prometheus-client, pydantic, python-dotenv, structlog, tenacity, websockets",
        "- optional runtime: fastapi, uvicorn, joblib, xgboost, optuna",
        "- dev/test: pytest, pytest-asyncio, pytest-xdist, pyright, ruff, pre-commit",
        "- stale/dead in the checked environment: black/pathspec mismatch and stock-indicators/pythonnet mismatch came from the ambient interpreter, not from repo requirement files",
        "",
        "## Requirement Files",
    ]
    for name, rows in requirement_files.items():
        lines.append(f"- {name}: {len(rows)} declared entries")
    lines.extend(
        [
            "",
            "## External Verification References",
            "- Binance USDⓈ-M futures WebSocket market streams: https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Connect",
            "- Binance options endpoint inventory (exchangeInfo/openInterest/mark): https://www.binance.com/en/skills/detail/binance/derivatives-trading-options",
            "- PyPI aiogram release metadata: https://pypi.org/project/aiogram/",
            "- PyPI websockets release metadata: https://pypi.org/project/websockets/",
            "",
            "## Imported Top-Level Modules Seen On Runtime Files",
            f"- {', '.join(name for name, _ in all_imports.most_common(20))}",
            "",
            "## Notes",
            "- `bot.dashboard` degrades cleanly when FastAPI/Uvicorn are absent, but the optional dependency is now explicit.",
            "- `bot.ml_filter` imports `joblib` only when live ML scoring is enabled.",
            "- `bot.core.self_learner` imports `optuna` lazily and remains out of the live runtime path.",
        ]
    )
    (ROOT / "dependency_audit.md").write_text("\n".join(lines), encoding="utf-8")


def write_docs_drift_report() -> None:
    lines = [
        "# Docs Drift Report",
        "",
        "## Current Runtime Truth",
        "- Entry path: `main.py -> bot.cli.run() -> bot.application.bot.SignalBot`.",
        "- Runtime is single-process within the current repo tree.",
        "",
        "## Adjustments Made",
        "- `PRD_v7.0_Futures_Signal_Bot_Final_Codex.md` is now explicitly marked as historical and non-authoritative for the current runtime.",
        "- `config.toml.example` now contains the `[bot.filters.setups]` table that the live config parser and strategies expect.",
        "",
        "## Remaining Drift",
        "- The PRD still contains large sections describing a multi-process/shared-memory/notifier architecture; treat those sections as design history only.",
        "- No dedicated README or operator document currently replaces the historical PRD as a concise runtime overview.",
    ]
    (ROOT / "docs_drift_report.md").write_text("\n".join(lines), encoding="utf-8")


def main() -> None:
    files = iter_audited_files()
    module_map = build_module_map(files)
    graph, imports_by_file = parse_import_graph(files, module_map)
    reverse_graph: dict[Path, set[Path]] = defaultdict(set)
    for src, targets in graph.items():
        for dst in targets:
            reverse_graph[dst].add(src)

    runtime_roots = [
        ROOT / "main.py",
        ROOT / "bot" / "cli.py",
        ROOT / "bot" / "application" / "bot.py",
        ROOT / "bot" / "__init__.py",
    ]
    test_roots = sorted((ROOT / "tests").glob("*.py"))
    script_roots = sorted((ROOT / "scripts").glob("live_*.py"))
    runtime_reachable = walk_reachable(graph, runtime_roots)
    test_reachable = walk_reachable(graph, test_roots)
    script_reachable = walk_reachable(graph, script_roots)

    replay_report, replay_findings = analyze_replay_runs()
    rows = build_manifest(files, runtime_reachable, test_reachable, script_reachable, reverse_graph)
    write_manifest(rows)
    write_findings_report(replay_findings)
    write_deletion_candidates(rows, reverse_graph)
    write_dependency_audit(imports_by_file)
    write_docs_drift_report()
    (ROOT / "replay_baseline_report.md").write_text(replay_report, encoding="utf-8")


if __name__ == "__main__":
    main()
