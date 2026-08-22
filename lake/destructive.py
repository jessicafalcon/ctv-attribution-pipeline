"""The destructive paths — ONE process that validates, confirms, then acts.

Three operations mutate state the pipeline cannot recompute from Kafka:
`reset` deletes a profile's lake of record, `replay` TRUNCATEs the two ClickHouse
serving tables before reloading them from the lake, `maintain` rewrites the lake's
data files (row content unchanged). Rounds 2 and 3 of the Phase-17 review each
found a new hole in a Make-level guard ($(origin) → MAKEFLAGS; separate recipe
lines → `make -i`): Make's variable model and multi-shell recipes cannot express
"validate, confirm, then act" as one unit. This module can. Every Makefile
recipe is one line that invokes it; `make -i` cannot step inside a process.

Threat model (DECISIONS Phase 17): these guards protect against MISTAKES — a
typo'd or empty PROFILE, a stray exported variable, a wrong-profile stack — not
against a user who controls the environment; that user can run `rm -rf`
directly. `MAKEFLAGS='CONFIRM=yes'` is therefore a stated residual, like
`$(shell …)` in an env-origin PROFILE.

What it guarantees: the profile is validated (`[a-z0-9_]+`, via
`lake.iceberg_catalog.configure`) BEFORE anything else; the root is derived from
the profile — there is no path argument to escape with; the prompt is answered
only by a tty (`--yes` skips it; `n`, EOF or a non-tty print "aborted" and exit
1); the action runs last. `LAKE_ROOT` in the environment is refused outright —
this CLI has no path input of any kind.
"""

import argparse
import os
import shutil
import sys
from collections.abc import Callable

from lake.iceberg_catalog import LakeRootUnset, _lake_root, configure, profile
from lake.read_attributed import PrePhase17RowError
from orchestration.replay import EmptyLakeError


def confirm_or_abort(action: str, *, yes: bool) -> None:
    """Print `aborted` and exit 1 unless `yes` or a tty user types `yes`."""
    if yes:
        return
    if not sys.stdin.isatty():
        print("aborted (no tty; pass CONFIRM=yes / --yes to skip the prompt)")
        sys.exit(1)
    try:
        answer = input(f"{action}. Type yes to continue: ")
    except EOFError:
        answer = ""
    if answer.strip() != "yes":
        print("aborted")
        sys.exit(1)


def reset(yes: bool) -> None:
    """`make lake-reset`: delete data/lake/<profile> — the lake of record for that
    profile. One of the three sanctioned destructive paths (spec D9)."""
    root = _lake_root()
    confirm_or_abort(
        f"lake-reset deletes {root.resolve()} (the lake of record for "
        f"PROFILE={profile()})",
        yes=yes,
    )
    # Let a failed removal RAISE (exit 1, no "removed"): a clean-stack target
    # that reports "removed" over a stale lake would load that lake's rows and
    # shift the pins silently (review gate, round 4). A missing root is fine.
    if not root.exists():
        print(f"lake-reset: nothing to remove at {root.resolve()}")
        return
    shutil.rmtree(root)
    print(f"lake-reset: removed {root.resolve()}")


def replay(yes: bool) -> None:
    """`make replay-serving`: TRUNCATE exposures_landed + attributed_conversions,
    then reload every day the lake holds (no broker). Refuses an empty lake
    before the prompt — truncating to reload nothing is data loss with a green
    exit code."""
    from orchestration.replay import lake_days, truncate_and_reload

    days = lake_days()
    if not days:
        raise EmptyLakeError(
            f"replay-serving: the lake for PROFILE={profile()} holds no days "
            "(never populated, or the wrong profile) — refusing to truncate the "
            "serving tables"
        )
    confirm_or_abort(
        "replay-serving TRUNCATEs exposures_landed + attributed_conversions and "
        f"reloads {len(days)} day(s) from the lake {_lake_root().resolve()}",
        yes=yes,
    )
    loaded = truncate_and_reload(days)
    # The eval_meta marker is stamped HERE, in the same process, after the load:
    # a separate recipe line would re-stamp it under `make -i` even when the
    # replay was refused or aborted — the wrong-marker hazard the PR-#25 guard
    # exists for (review gate, round 4).
    from clickhouse.write_marker import write_marker

    write_marker(profile())
    print(
        f"replay-serving: {loaded['exposures']} exposures, "
        f"{loaded['attributed']} attributed rows reloaded from the lake (no broker); "
        f"eval_meta: marker set to profile={profile()}"
    )


def maintain(yes: bool) -> None:
    """`make lake-maintain`: the `lake_maintenance` Dagster job — rewrite each day
    partition that accumulated small files (row content unchanged, data files
    rewritten) and expire old snapshots. A rewrite of the record, so it prompts
    like the other two paths."""
    from orchestration.maintenance import lake_maintenance

    confirm_or_abort(
        f"lake-maintain rewrites the data files of {_lake_root().resolve()} "
        "(row content unchanged) and expires old snapshots",
        yes=yes,
    )
    result = lake_maintenance.execute_in_process()
    if not result.success:
        raise RuntimeError("lake_maintenance job failed")
    for name in ("maintain_exposures", "maintain_attributed"):
        print(f"lake-maintain {name}: {result.output_for_node(name)}")


ACTIONS: dict[str, Callable[[bool], None]] = {
    "reset": reset,
    "replay": replay,
    "maintain": maintain,
}


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="the three destructive paths")
    parser.add_argument("action", choices=sorted(ACTIONS))
    parser.add_argument(
        "--profile", required=True, help="binds the lake of record: data/lake/<profile>"
    )
    parser.add_argument("--yes", action="store_true", help="skip the prompt")
    args = parser.parse_args(argv)
    if os.environ.get("LAKE_ROOT"):
        # Refused outright here, pytest or not: the destructive CLI derives its
        # root from --profile and nothing else (closes the forged
        # PYTEST_CURRENT_TEST path; the subprocess tests scrub both variables).
        sys.exit(
            f"{args.action}: refusing — LAKE_ROOT is set; this CLI binds its "
            "lake from --profile only"
        )
    try:
        configure(args.profile)  # validates [a-z0-9_]+ BEFORE anything else
        ACTIONS[args.action](args.yes)
    except (LakeRootUnset, EmptyLakeError, PrePhase17RowError) as e:
        # Refusals — the explicit refusal classes only, never bare ValueError
        # (a pydantic ValidationError would masquerade as one) — are one line +
        # exit 1. Anything else (a PermissionError from rmtree, a ClickHouse
        # error) is a FAILURE and propagates with its traceback.
        sys.exit(f"{args.action}: refusing — {e}")


if __name__ == "__main__":
    main()
