from pathlib import Path

import pytest
from connectors.base import ComplianceError
from connectors.local_file import LocalFileConnector


def test_local_file_connector_copies_file(tmp_path: Path) -> None:
    source = tmp_path / "source.csv"
    source.write_text("a,b\n1,2\n", encoding="utf-8")
    connector = LocalFileConnector(cache_dir=tmp_path / "cache", user_agent="ua")
    result = connector.fetch({"path": str(source)}, run_dir=tmp_path / "run", html_allowlist=[])
    assert result.local_path.exists()
    assert result.local_path.read_text(encoding="utf-8").startswith("a,b")


def test_local_file_connector_missing_file(tmp_path: Path) -> None:
    connector = LocalFileConnector(cache_dir=tmp_path / "cache", user_agent="ua")
    with pytest.raises(ComplianceError):
        connector.fetch(
            {"path": str(tmp_path / "missing.csv")},
            run_dir=tmp_path / "run",
            html_allowlist=[],
        )

