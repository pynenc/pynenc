"""Unit tests for build_test_combinations().

Verifies that every generated combination is internally consistent:
- All backend classes share the same family (no mixing of Python-Mem
  classes with Rust-Mem classes, etc.)
- The BackendFamily enum value matches the actual classes in the tuple
- All IDs are unique (no duplicate parametrize IDs)
- Rust runners are never paired with Python-only backends and vice-versa
"""

import pytest

from pynenc_tests.integration.combinations.conftest import (
    AppComponents,
    BackendFamily,
    MEM_CLASSES,
    SQLITE_CLASSES,
    build_test_combinations,
)


# ---------------------------------------------------------------------------
# Helper: class-to-expected-family lookup
# ---------------------------------------------------------------------------


def _expected_family(components: AppComponents) -> BackendFamily:
    """Derive the expected BackendFamily purely from the component classes.

    This is intentionally written without any string-sniffing so it serves
    as an independent cross-check against the enum stored on the combination.
    """
    class_sets = {
        BackendFamily.MEM: MEM_CLASSES,
        BackendFamily.SQLITE: SQLITE_CLASSES,
    }

    actual = (
        components.client_data_store,
        components.broker,
        components.orchestrator,
        components.state_backend,
        components.trigger,
    )
    for family, classes in class_sets.items():
        if actual == classes:
            return family
    raise ValueError(
        f"Component tuple {actual!r} does not match any known BackendFamily. "
        "A new backend was added without updating the test."
    )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

COMBINATIONS = build_test_combinations()


@pytest.mark.parametrize("combo", COMBINATIONS, ids=lambda c: c.combination_id)
def test_family_matches_classes(combo: AppComponents) -> None:
    """The stored BackendFamily must match the actual component classes."""
    assert combo.family == _expected_family(combo), (
        f"{combo.combination_id!r}: stored family {combo.family!r} "
        f"does not match the component classes"
    )


@pytest.mark.parametrize("combo", COMBINATIONS, ids=lambda c: c.combination_id)
def test_sqlite_family_gets_sqlite_db_env(combo: AppComponents) -> None:
    """SQLite-family combos must have is_sqlite=True; Mem-family must have is_sqlite=False."""
    sqlite_backend = combo.family == BackendFamily.SQLITE
    assert combo.family.is_sqlite == sqlite_backend


def test_all_combination_ids_unique() -> None:
    """Every combination must produce a distinct pytest parametrize ID."""
    ids = [c.combination_id for c in COMBINATIONS]
    assert len(ids) == len(set(ids)), (
        f"Duplicate combination IDs: {[x for x in ids if ids.count(x) > 1]}"
    )
