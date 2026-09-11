"""Deterministic QA unit-test workflow runner for Python data pipelines."""

from __future__ import annotations

import csv
import re
import subprocess
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable


FILE_AUDIT_HEADERS = [
    "test_name",
    "status",
    "test_type",
    "created_date",
    "updated_date",
    "last_executed",
    "source_file",
    "error_message",
]

EXECUTION_SUMMARY_HEADERS = [
    "execution_date",
    "source_file",
    "file_status",
    "tests_added",
    "tests_updated",
    "total_tests",
    "passed",
    "failed",
    "skipped",
    "coverage_percent",
    "csv_updated",
    "remarks",
]

COVERAGE_SUMMARY_HEADERS = [
    "execution_date",
    "source_file",
    "total_lines",
    "covered_lines",
    "missed_lines",
    "coverage_percent",
]


@dataclass(frozen=True)
class PytestStats:
    passed: int = 0
    failed: int = 0
    skipped: int = 0
    errors: int = 0

    @property
    def total(self) -> int:
        return self.passed + self.failed + self.skipped + self.errors


@dataclass(frozen=True)
class CoverageStats:
    total_lines: int = 0
    covered_lines: int = 0
    missed_lines: int = 0
    coverage_percent: float = 0.0


def list_source_files(src_dir: Path) -> list[Path]:
    if not src_dir.exists():
        return []
    return sorted(
        path
        for path in src_dir.rglob("*.py")
        if path.is_file() and path.name != "__init__.py"
    )


def expected_test_file(source_file: Path, src_dir: Path, tests_dir: Path) -> Path:
    rel_path = source_file.relative_to(src_dir).with_suffix("")
    test_name = "test_" + "_".join(rel_path.parts) + ".py"
    return tests_dir / test_name


def _ensure_csv_file(path: Path, headers: Iterable[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not path.exists():
        with path.open("w", newline="", encoding="utf-8") as csv_file:
            writer = csv.DictWriter(csv_file, fieldnames=list(headers))
            writer.writeheader()


def _load_csv_rows(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", newline="", encoding="utf-8") as csv_file:
        return list(csv.DictReader(csv_file))


def _write_csv_rows(path: Path, headers: Iterable[str], rows: list[dict[str, str]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=list(headers))
        writer.writeheader()
        writer.writerows(rows)


def parse_pytest_summary(output: str) -> PytestStats:
    summary_line = ""
    for line in output.splitlines():
        if " in " in line and ("passed" in line or "failed" in line or "error" in line):
            summary_line = line
    if not summary_line:
        return PytestStats()

    def _extract(term: str) -> int:
        match = re.search(rf"(\d+)\s+{term}", summary_line)
        return int(match.group(1)) if match else 0

    return PytestStats(
        passed=_extract("passed"),
        failed=_extract("failed"),
        skipped=_extract("skipped"),
        errors=_extract("error|errors"),
    )


def parse_coverage_for_file(output: str, source_file: Path) -> CoverageStats:
    source_name = source_file.as_posix()
    line_pattern = re.compile(
        rf"^\s*{re.escape(source_name)}\s+(\d+)\s+(\d+)\s+(\d+)%(?:\s+.*)?$"
    )
    for line in output.splitlines():
        match = line_pattern.match(line.strip())
        if not match:
            continue
        total = int(match.group(1))
        missed = int(match.group(2))
        covered = total - missed
        coverage_percent = float(match.group(3))
        return CoverageStats(
            total_lines=total,
            covered_lines=covered,
            missed_lines=missed,
            coverage_percent=coverage_percent,
        )
    return CoverageStats()


def run_pytest_command(args: list[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(args, text=True, capture_output=True, check=False)


def update_file_audit_csv(
    csv_path: Path,
    source_file: str,
    test_file: str,
    stats: PytestStats,
    execution_ts: str,
    error_message: str,
    test_file_was_created: bool,
) -> None:
    _ensure_csv_file(csv_path, FILE_AUDIT_HEADERS)
    rows = _load_csv_rows(csv_path)
    existing = {row["test_name"]: row for row in rows}
    test_name = Path(test_file).stem
    status = "PASSED" if stats.failed == 0 and stats.errors == 0 else "FAILED"
    if stats.total == 0:
        status = "SKIPPED"

    previous = existing.get(test_name)
    created_date = previous["created_date"] if previous else execution_ts
    updated_date = execution_ts if (test_file_was_created or previous is None) else (
        previous.get("updated_date") or execution_ts
    )

    existing[test_name] = {
        "test_name": test_name,
        "status": status,
        "test_type": "unit",
        "created_date": created_date,
        "updated_date": updated_date,
        "last_executed": execution_ts,
        "source_file": source_file,
        "error_message": error_message,
    }
    _write_csv_rows(csv_path, FILE_AUDIT_HEADERS, list(existing.values()))


def append_execution_summary(
    csv_path: Path,
    *,
    execution_ts: str,
    source_file: str,
    file_status: str,
    tests_added: int,
    tests_updated: int,
    stats: PytestStats,
    coverage_percent: float,
    csv_updated: str,
    remarks: str,
) -> None:
    _ensure_csv_file(csv_path, EXECUTION_SUMMARY_HEADERS)
    with csv_path.open("a", newline="", encoding="utf-8") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=EXECUTION_SUMMARY_HEADERS)
        writer.writerow(
            {
                "execution_date": execution_ts,
                "source_file": source_file,
                "file_status": file_status,
                "tests_added": tests_added,
                "tests_updated": tests_updated,
                "total_tests": stats.total,
                "passed": stats.passed,
                "failed": stats.failed + stats.errors,
                "skipped": stats.skipped,
                "coverage_percent": f"{coverage_percent:.2f}",
                "csv_updated": csv_updated,
                "remarks": remarks,
            }
        )


def append_coverage_summary(
    csv_path: Path,
    execution_ts: str,
    source_file: str,
    coverage: CoverageStats,
) -> None:
    _ensure_csv_file(csv_path, COVERAGE_SUMMARY_HEADERS)
    with csv_path.open("a", newline="", encoding="utf-8") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=COVERAGE_SUMMARY_HEADERS)
        writer.writerow(
            {
                "execution_date": execution_ts,
                "source_file": source_file,
                "total_lines": coverage.total_lines,
                "covered_lines": coverage.covered_lines,
                "missed_lines": coverage.missed_lines,
                "coverage_percent": f"{coverage.coverage_percent:.2f}",
            }
        )


def generate_test_stub(source_file: Path, src_dir: Path, test_file: Path) -> bool:
    if test_file.exists():
        return False
    test_file.parent.mkdir(parents=True, exist_ok=True)
    module_path = ".".join(source_file.relative_to(src_dir).with_suffix("").parts)
    test_file.write_text(
        "\n".join(
            [
                '"""Auto-generated deterministic smoke tests; extend with business-rule assertions."""',
                "",
                "import importlib",
                "",
                "",
                f"MODULE_NAME = 'src.{module_path}'",
                "",
                "",
                "def test_module_imports() -> None:",
                "    module = importlib.import_module(MODULE_NAME)",
                "    assert module is not None",
                "",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    return True


def run_workflow(
    repo_root: Path,
    src_dir_name: str = "src",
    tests_dir_name: str = "tests",
    results_dir_name: str = "test_results",
) -> dict[str, object]:
    src_dir = repo_root / src_dir_name
    tests_dir = repo_root / tests_dir_name
    results_dir = repo_root / results_dir_name

    execution_ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    files_scanned = 0
    tests_added_total = 0
    tests_updated_total = 0
    passed_total = 0
    failed_total = 0
    coverage_total = 0.0
    coverage_count = 0
    file_rows: list[dict[str, object]] = []

    for source_file in list_source_files(src_dir):
        files_scanned += 1
        test_file = expected_test_file(source_file, src_dir, tests_dir)
        test_was_created = generate_test_stub(source_file, src_dir, test_file)
        tests_added = 1 if test_was_created else 0
        tests_added_total += tests_added

        pytest_base_cmd = [sys.executable, "-m", "pytest"]
        test_result = run_pytest_command(pytest_base_cmd + [str(test_file)])
        stats = parse_pytest_summary(test_result.stdout + "\n" + test_result.stderr)
        passed_total += stats.passed
        failed_total += stats.failed + stats.errors
        error_message = "" if test_result.returncode == 0 else (test_result.stderr or test_result.stdout).strip()

        coverage_result = run_pytest_command(
            [
                *pytest_base_cmd,
                str(test_file),
                f"--cov=src.{'.'.join(source_file.relative_to(src_dir).with_suffix('').parts)}",
                "--cov-report=term-missing",
            ]
        )
        coverage = parse_coverage_for_file(
            coverage_result.stdout + "\n" + coverage_result.stderr,
            source_file.relative_to(repo_root),
        )
        if coverage.total_lines > 0:
            coverage_total += coverage.coverage_percent
            coverage_count += 1

        source_key = source_file.relative_to(repo_root).as_posix()
        update_file_audit_csv(
            csv_path=results_dir / f"{source_file.stem}.csv",
            source_file=source_key,
            test_file=str(test_file.relative_to(repo_root)),
            stats=stats,
            execution_ts=execution_ts,
            error_message=error_message,
            test_file_was_created=test_was_created,
        )
        append_execution_summary(
            csv_path=results_dir / "execution_summary.csv",
            execution_ts=execution_ts,
            source_file=source_key,
            file_status="NEW" if test_was_created else "NO_CHANGE",
            tests_added=tests_added,
            tests_updated=0,
            stats=stats,
            coverage_percent=coverage.coverage_percent,
            csv_updated="YES",
            remarks="REVIEW_REQUIRED: only deterministic smoke assertions generated.",
        )
        append_coverage_summary(
            csv_path=results_dir / "coverage_summary.csv",
            execution_ts=execution_ts,
            source_file=source_key,
            coverage=coverage,
        )
        file_rows.append(
            {
                "source_file": source_key,
                "new_tests": tests_added,
                "updated_tests": 0,
                "passed": stats.passed,
                "failed": stats.failed + stats.errors,
                "coverage_percent": f"{coverage.coverage_percent:.2f}",
                "status": "NEW" if test_was_created else "NO_CHANGE",
            }
        )

    coverage_avg = (coverage_total / coverage_count) if coverage_count else 0.0
    return {
        "execution_date": execution_ts,
        "files_scanned": files_scanned,
        "tests_added": tests_added_total,
        "tests_updated": tests_updated_total,
        "passed": passed_total,
        "failed": failed_total,
        "coverage_avg": f"{coverage_avg:.2f}",
        "csv_reports_updated": "YES",
        "file_rows": file_rows,
    }


def render_markdown_summary(run_summary: dict[str, object]) -> str:
    run_table = (
        "### Table 1: Run Summary\n"
        "| Execution Date | Files Scanned | Tests Added | Tests Updated | Passed | Failed | Coverage Avg | CSV Reports Updated |\n"
        "| --- | --- | --- | --- | --- | --- | --- | --- |\n"
        f"| {run_summary['execution_date']} | {run_summary['files_scanned']} | {run_summary['tests_added']} | "
        f"{run_summary['tests_updated']} | {run_summary['passed']} | {run_summary['failed']} | "
        f"{run_summary['coverage_avg']} | {run_summary['csv_reports_updated']} |\n"
    )
    file_header = (
        "\n### Table 2: File-Level Summary\n"
        "| Source File | New Tests | Updated Tests | Passed | Failed | Coverage % | Status |\n"
        "| --- | --- | --- | --- | --- | --- | --- |\n"
    )
    file_rows = run_summary.get("file_rows", [])
    if not file_rows:
        file_rows_text = "| N/A | 0 | 0 | 0 | 0 | 0.00 | REVIEW_REQUIRED |\n"
    else:
        file_rows_text = "".join(
            f"| {row['source_file']} | {row['new_tests']} | {row['updated_tests']} | {row['passed']} | {row['failed']} | "
            f"{row['coverage_percent']} | {row['status']} |\n"
            for row in file_rows
        )
    return run_table + file_header + file_rows_text


if __name__ == "__main__":
    summary = run_workflow(Path.cwd())
    print(render_markdown_summary(summary))
