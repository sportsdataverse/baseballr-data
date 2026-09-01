"""models/manifest.yaml is the single home for the model-family list (Track C step 2).

Per-row biting guards: manifest ↔ numbered stage scripts ↔ `_CARD_META`
(the grain/source/gate authority) ↔ models/REGISTRY.md.
"""

from importlib import import_module
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
MANIFEST = ROOT / "models" / "manifest.yaml"
REGISTRY = ROOT / "models" / "REGISTRY.md"
STAGES_DIR = ROOT / "python"


def _models() -> dict:
    return yaml.safe_load(MANIFEST.read_text(encoding="utf-8"))["suites"]["mlb"]["models"]


def test_manifest_parses_and_driver_exists():
    doc = yaml.safe_load(MANIFEST.read_text(encoding="utf-8"))
    assert (ROOT / doc["driver"]).is_file()
    assert set(doc["suites"]) == {"mlb"}


def test_stages_and_manifest_agree_bidirectionally():
    files = {p.stem for p in STAGES_DIR.glob("mlb_model_[0-9][0-9]_*.py")}
    manifest = {Path(m["stage"]).stem for m in _models().values()}
    assert files == manifest, f"files-only={files - manifest}, manifest-only={manifest - files}"
    for name, m in _models().items():
        assert (ROOT / m["stage"]).is_file(), f"{name} stage missing"


def test_manifest_tags_match_card_meta_exactly():
    from mlb_model_publish.builders import _CARD_META

    manifest_tags = {m["release_tag"] for m in _models().values()}
    assert manifest_tags == set(_CARD_META), (
        f"manifest-only={manifest_tags - set(_CARD_META)}, "
        f"card-meta-only={set(_CARD_META) - manifest_tags}"
    )


def test_stage_modules_import_expose_main_and_inject_their_subcommand():
    for name, m in _models().items():
        stem = Path(m["stage"]).stem
        mod = import_module(f"{stem}")
        assert callable(getattr(mod, "main", None)), f"{stem} has no main()"
        src = (ROOT / m["stage"]).read_text(encoding="utf-8")
        assert f'"{m["subcommand"]}"' in src, f"{stem} does not inject {m['subcommand']!r}"


def test_registry_names_every_family_tag():
    registry = REGISTRY.read_text(encoding="utf-8")
    for name, m in _models().items():
        assert m["release_tag"] in registry, f"{name} tag not in REGISTRY.md"
