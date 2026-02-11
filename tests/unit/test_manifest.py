from pathlib import Path

from hdb.manifest import build_digests, license_record_text


def test_build_digests(tmp_path: Path) -> None:
    file_path = tmp_path / "a.txt"
    file_path.write_text("abc", encoding="utf-8")
    digests = build_digests([file_path])
    assert len(digests) == 1
    assert len(digests[0].sha256) == 64


def test_license_record_contains_fields() -> None:
    text = license_record_text("CC BY 4.0", "https://license.example", "Publisher")
    assert "CC BY 4.0" in text
    assert "https://license.example" in text
    assert "Publisher" in text
