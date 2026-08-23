"""Live: the eval profile/DB-mismatch guard fires against a real ClickHouse
(BACKLOG 43). Pins the DB-glue path `accuracy/guard.py db_profile_marker` →
`assert_profile_marker` that the offline unit test (`tests/test_eval_guard.py`)
bypasses (it calls the pure function directly). Skipped when ClickHouse is
unreachable, so `make test` stays green offline; runs under `make test-int`.

CI-safe on the shared tiny stack: both tests re-stamp profile=tiny (idempotent,
the same marker `make run` wrote) and the mismatch case raises BEFORE any scoring
touches the serving tables — no mutation, no order dependence.
"""

import pytest

from accuracy.guard import ProfileMismatchError
from accuracy.run import main as eval_main
from clickhouse.apply import apply as apply_ddl
from clickhouse.client import connect
from clickhouse.write_marker import write_marker


@pytest.fixture(scope="module", autouse=True)
def _require_clickhouse() -> None:
    try:
        connect().command("select 1")
    except Exception:
        pytest.skip("ClickHouse unreachable; runs under make test-int")


def _stamp_tiny() -> None:
    """Ensure `eval_meta` exists and holds profile=tiny — what `make run` on the
    tiny test-int stack already wrote. Idempotent, safe to re-run."""
    client = connect()
    apply_ddl(client)
    write_marker("tiny", client)


def test_eval_scores_the_matching_profile() -> None:
    # marker == --profile → the guard passes and eval runs against the
    # tiny-populated serving tables (make run precedes make test-int). Must not
    # raise.
    _stamp_tiny()
    eval_main(["--profile", "tiny"])


def test_eval_refuses_a_profile_db_mismatch() -> None:
    # the exact original bug: scoring long_delay against the tiny-populated DB.
    # marker (tiny) != --profile (long_delay) → loud exit before any scoring, so
    # this never touches (nonexistent) long_delay data.
    _stamp_tiny()
    with pytest.raises(ProfileMismatchError) as exc:
        eval_main(["--profile", "long_delay"])
    assert "profile/DB mismatch" in str(exc.value)


def test_eval_missing_marker_fails_loud(monkeypatch: pytest.MonkeyPatch) -> None:
    # The live None-path: point eval at a database that has no eval_meta, so the
    # DB read (`exists table eval_meta` → 0) yields None and the guard gives its
    # friendly "no marker" exit rather than a raw UNKNOWN_TABLE. Non-destructive —
    # it never truncates or drops the real marker table (destructive-command
    # rule); `system` always exists and never holds eval_meta.
    monkeypatch.setenv("CLICKHOUSE_DB", "system")
    with pytest.raises(ProfileMismatchError) as exc:
        eval_main(["--profile", "tiny"])
    assert "no eval_meta profile marker" in str(exc.value)


def test_write_marker_is_idempotent_on_read() -> None:
    # Re-stamping the same profile converges to a single row on FINAL read
    # (ReplacingMergeTree keyed on the constant k=0), so a replay from offset 0
    # leaves one marker, not a pile. Restores tiny for any later test/eval.
    client = connect()
    apply_ddl(client)
    write_marker("tiny", client)
    write_marker("tiny", client)
    write_marker("tiny", client)
    assert client.query("select count() from eval_meta final").result_rows[0][0] == 1
    assert (
        client.query("select profile from eval_meta final").result_rows[0][0] == "tiny"
    )
