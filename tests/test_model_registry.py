"""models/REGISTRY.md stays in lockstep with _CARD_META (the code authority).

Bites per family AND per gate name: delete a family's row, or rename a gate in
_CARD_META without updating the registry, and this fails. Values live in
_CARD_META; the registry cites them — this guard checks NAMES, not numbers, so
a deliberate gate re-derivation only needs editing in one place plus the row.
"""

from pathlib import Path

from mlb_model_publish.builders import _CARD_META

REGISTRY = Path(__file__).resolve().parents[1] / "models" / "REGISTRY.md"


def _rows() -> list[str]:
    text = REGISTRY.read_text(encoding="utf-8")
    return [ln for ln in text.splitlines() if ln.startswith("|") and "---" not in ln]


def test_registry_exists():
    assert REGISTRY.is_file(), "models/REGISTRY.md is missing"


def test_every_card_meta_family_has_a_row():
    rows = _rows()
    missing = [tag for tag in _CARD_META if not any(tag in r for r in rows)]
    assert not missing, f"_CARD_META families with no registry TABLE ROW: {missing}"


def test_every_gate_name_appears_in_its_family_row():
    rows = _rows()
    problems = []
    for tag, meta in _CARD_META.items():
        row = next((r for r in rows if tag in r), "")
        for gate in meta.get("gates", {}):
            if gate not in row:
                problems.append(f"{tag}: gate {gate} not in its registry row")
    assert not problems, problems
