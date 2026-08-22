"""The lake root is owned by --profile; there is no default root (review gate).

An unset default once produced a profile-mixed lake at data/lake/ that
`make lake-reset` refuses to touch. Now: `configure(profile)` binds
data/lake/<profile>; LAKE_ROOT is test-only (tmp fixtures) and an entry point
refuses it outside pytest; with neither, any lake access raises `LakeRootUnset`.
"""

from pathlib import Path

import pytest

from lake import iceberg_catalog as cat


@pytest.fixture(autouse=True)
def _reset_profile(monkeypatch):
    monkeypatch.setattr(cat, "_profile", None)
    monkeypatch.delenv("LAKE_ROOT", raising=False)


def test_no_profile_and_no_fixture_root_raises_not_defaults() -> None:
    with pytest.raises(cat.LakeRootUnset, match="--profile"):
        cat._lake_root()
    with pytest.raises(cat.LakeRootUnset):
        cat.ensure_exposures()


def test_configure_binds_data_lake_profile() -> None:
    assert cat.configure("long_delay") == Path("data/lake/long_delay")
    assert cat._lake_root() == Path("data/lake/long_delay")
    assert cat.profile() == "long_delay"


@pytest.mark.parametrize("bad", ["", "../x", "/abs", "Tiny", "my-profile", "a b"])
def test_configure_refuses_a_malformed_profile(bad: str) -> None:
    with pytest.raises(cat.LakeRootUnset, match="not \\[a-z0-9_\\]"):
        cat.configure(bad)


def test_lake_root_env_is_test_only(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("LAKE_ROOT", str(tmp_path))
    assert cat._lake_root() == tmp_path  # a fixture override wins under pytest
    monkeypatch.delenv("PYTEST_CURRENT_TEST")  # … but an entry point outside pytest
    with pytest.raises(cat.LakeRootUnset, match="test-only"):
        cat.configure("tiny")


def test_catalog_exists_does_not_create_one(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("LAKE_ROOT", str(tmp_path / "lake"))
    assert cat.catalog_exists() is False
    assert not (tmp_path / "lake").exists()
