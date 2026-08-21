"""Offline unit tests for the eval profile/DB-mismatch guard (BACKLOG 43). Pure —
no database (the guard logic is extracted to `accuracy/guard.py` for exactly
this)."""

import pytest

from accuracy.guard import ProfileMismatchError, assert_profile_marker


def test_matching_profile_passes() -> None:
    assert assert_profile_marker("long_delay", "long_delay") is None


def test_missing_marker_raises_loud() -> None:
    with pytest.raises(ProfileMismatchError) as exc:
        assert_profile_marker(None, "tiny")
    assert "no eval_meta profile marker" in str(exc.value)


def test_mismatch_raises_loud() -> None:
    # the exact original bug: tiny reference labels scored against a
    # long_delay-populated DB. A conversion_id-subset check would false-pass this
    # (tiny's ids are a subset of long_delay's); the marker catches it.
    with pytest.raises(ProfileMismatchError) as exc:
        assert_profile_marker("long_delay", "tiny")
    message = str(exc.value)
    assert "profile/DB mismatch" in message
    assert "long_delay" in message and "tiny" in message
