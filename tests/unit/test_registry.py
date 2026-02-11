from pathlib import Path

from hdb.registry import load_registry


def test_registry_parses_dataset() -> None:
    registry = load_registry(Path("datasets/registry.yaml"))
    dataset = registry.dataset_map()["demo_dataset"]
    assert dataset.title.startswith("Life Expectancy")
    assert dataset.output_schemas.canonical == "canonical_v1"


def test_registry_html_allowlist_present() -> None:
    registry = load_registry(Path("datasets/registry.yaml"))
    assert "ourworldindata.org" in registry.html_allowlist
