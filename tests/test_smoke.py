"""Step 1 gate: the package imports, and the CLI answers --help/--version."""

import subprocess
import sys

import tempo_tb_ingest


def test_version_attribute() -> None:
    assert tempo_tb_ingest.__version__


def _run_cli(*args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, "-m", "tempo_tb_ingest", *args],
        capture_output=True,
        text=True,
        timeout=30,
    )


def test_cli_help_exits_zero() -> None:
    result = _run_cli("--help")
    assert result.returncode == 0
    assert "Tempo-BT" in result.stdout


def test_cli_version_exits_zero() -> None:
    result = _run_cli("--version")
    assert result.returncode == 0
    assert tempo_tb_ingest.__version__ in result.stdout


def test_unimplemented_commands_fail_loudly() -> None:
    # placeholders must not pretend to succeed (no-silent-success rule)
    for command in ("daemon", "promote"):
        result = _run_cli(command)
        assert result.returncode == 2, f"{command} should exit 2 until implemented"
        assert "not implemented" in result.stderr
