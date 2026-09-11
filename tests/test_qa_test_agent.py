from __future__ import annotations

import subprocess
from pathlib import Path

from src import qa_test_agent
from src.qa_test_agent import (
    CoverageStats,
    PytestStats,
    append_coverage_summary,
    append_execution_summary,
    expected_test_file,
    generate_test_stub,
    list_source_files,
    parse_coverage_for_file,
    parse_pytest_summary,
    render_markdown_summary,
    run_pytest_command,
    run_workflow,
    update_file_audit_csv,
)


def test_expected_test_file_nested_path() -> None:
    src_dir = Path("/repo/src")
    tests_dir = Path("/repo/tests")
    source_file = Path("/repo/src/customer/transform_customer.py")
    assert (
        expected_test_file(source_file, src_dir, tests_dir)
        == Path("/repo/tests/test_customer_transform_customer.py")
    )


def test_parse_pytest_summary_extracts_counts() -> None:
    output = "================ 3 passed, 1 failed, 2 skipped in 0.33s ================"
    stats = parse_pytest_summary(output)
    assert stats == PytestStats(passed=3, failed=1, skipped=2, errors=0)


def test_parse_coverage_for_file_extracts_file_coverage() -> None:
    output = "\n".join(
        [
            "Name Stmts Miss Cover",
            "src/qa_test_agent.py 10 2 80%",
        ]
    )
    coverage = parse_coverage_for_file(output, Path("src/qa_test_agent.py"))
    assert coverage == CoverageStats(total_lines=10, covered_lines=8, missed_lines=2, coverage_percent=80.0)
    with_missing_column = "src/qa_test_agent.py 10 2 80%   42-43"
    assert parse_coverage_for_file(with_missing_column, Path("src/qa_test_agent.py")).coverage_percent == 80.0


def test_list_source_files_handles_missing_and_filters_init(tmp_path: Path) -> None:
    assert list_source_files(tmp_path / "does_not_exist") == []
    src_dir = tmp_path / "src"
    src_dir.mkdir()
    (src_dir / "__init__.py").write_text("", encoding="utf-8")
    nested = src_dir / "mod"
    nested.mkdir()
    file_path = nested / "logic.py"
    file_path.write_text("value = 1\n", encoding="utf-8")
    assert list_source_files(src_dir) == [file_path]


def test_parse_pytest_summary_empty_output() -> None:
    assert parse_pytest_summary("collection output only") == PytestStats()


def test_parse_coverage_for_file_no_match_returns_default() -> None:
    assert parse_coverage_for_file("TOTAL 1 1 0%", Path("src/missing.py")) == CoverageStats()


def test_run_pytest_command_executes_subprocess() -> None:
    result = run_pytest_command(["python", "-c", "print('ok')"])
    assert result.returncode == 0
    assert "ok" in result.stdout


def test_update_file_audit_csv_preserves_created_date(tmp_path: Path) -> None:
    csv_path = tmp_path / "qa_test_agent.csv"
    update_file_audit_csv(
        csv_path=csv_path,
        source_file="src/qa_test_agent.py",
        test_file="tests/test_qa_test_agent.py",
        stats=PytestStats(passed=1),
        execution_ts="2026-09-11 10:00:00",
        error_message="",
        test_file_was_created=True,
    )
    update_file_audit_csv(
        csv_path=csv_path,
        source_file="src/qa_test_agent.py",
        test_file="tests/test_qa_test_agent.py",
        stats=PytestStats(passed=2),
        execution_ts="2026-09-11 11:00:00",
        error_message="",
        test_file_was_created=False,
    )
    lines = csv_path.read_text(encoding="utf-8").splitlines()
    assert len(lines) == 2
    assert "2026-09-11 10:00:00" in lines[1]
    assert "2026-09-11 11:00:00" in lines[1]


def test_update_file_audit_csv_skipped_status(tmp_path: Path) -> None:
    csv_path = tmp_path / "qa_test_agent.csv"
    update_file_audit_csv(
        csv_path=csv_path,
        source_file="src/qa_test_agent.py",
        test_file="tests/test_qa_test_agent.py",
        stats=PytestStats(),
        execution_ts="2026-09-11 11:00:00",
        error_message="",
        test_file_was_created=False,
    )
    assert "SKIPPED" in csv_path.read_text(encoding="utf-8")


def test_append_summary_csvs(tmp_path: Path) -> None:
    execution_csv = tmp_path / "execution_summary.csv"
    append_execution_summary(
        execution_csv,
        execution_ts="2026-09-11 12:00:00",
        source_file="src/qa_test_agent.py",
        file_status="NO_CHANGE",
        tests_added=0,
        tests_updated=1,
        stats=PytestStats(passed=4, failed=1),
        coverage_percent=80.0,
        csv_updated="YES",
        remarks="ok",
    )
    coverage_csv = tmp_path / "coverage_summary.csv"
    append_coverage_summary(
        coverage_csv,
        execution_ts="2026-09-11 12:00:00",
        source_file="src/qa_test_agent.py",
        coverage=CoverageStats(total_lines=5, covered_lines=5, missed_lines=0, coverage_percent=100.0),
    )
    assert "coverage_percent" in execution_csv.read_text(encoding="utf-8").splitlines()[0]
    assert "100.00" in coverage_csv.read_text(encoding="utf-8")


def test_generate_test_stub_creates_file_once(tmp_path: Path) -> None:
    src_dir = tmp_path / "src"
    src_dir.mkdir()
    source_file = src_dir / "sample.py"
    source_file.write_text("def f():\n    return 1\n", encoding="utf-8")
    test_file = tmp_path / "tests" / "test_sample.py"
    assert generate_test_stub(source_file, src_dir, test_file) is True
    assert "MODULE_NAME = 'src.sample'" in test_file.read_text(encoding="utf-8")
    assert generate_test_stub(source_file, src_dir, test_file) is False


def test_run_workflow_generates_csvs_and_summary(tmp_path: Path, monkeypatch) -> None:
    repo_root = tmp_path / "repo"
    src_dir = repo_root / "src"
    src_dir.mkdir(parents=True)
    (src_dir / "sample.py").write_text("def sample():\n    return 1\n", encoding="utf-8")
    (src_dir / "__init__.py").write_text("", encoding="utf-8")
    (repo_root / "tests").mkdir()

    def fake_run(args: list[str]) -> subprocess.CompletedProcess[str]:
        if "--cov-report=term-missing" in args:
            return subprocess.CompletedProcess(
                args=args,
                returncode=0,
                stdout="src/sample.py 5 0 100%",
                stderr="",
            )
        return subprocess.CompletedProcess(
            args=args,
            returncode=0,
            stdout="1 passed in 0.01s",
            stderr="",
        )

    monkeypatch.setattr(qa_test_agent, "run_pytest_command", fake_run)

    summary = run_workflow(repo_root)
    assert summary["files_scanned"] == 1
    assert summary["tests_added"] == 1
    assert summary["failed"] == 0

    file_audit = repo_root / "test_results" / "sample.csv"
    execution = repo_root / "test_results" / "execution_summary.csv"
    coverage = repo_root / "test_results" / "coverage_summary.csv"
    assert file_audit.exists()
    assert execution.exists()
    assert coverage.exists()

    markdown = render_markdown_summary(summary)
    assert "Table 1: Run Summary" in markdown
    assert "src/sample.py" in markdown


def test_render_markdown_summary_no_files_row() -> None:
    markdown = render_markdown_summary(
        {
            "execution_date": "2026-09-11 12:00:00",
            "files_scanned": 0,
            "tests_added": 0,
            "tests_updated": 0,
            "passed": 0,
            "failed": 0,
            "coverage_avg": "0.00",
            "csv_reports_updated": "YES",
            "file_rows": [],
        }
    )
    assert "REVIEW_REQUIRED" in markdown
