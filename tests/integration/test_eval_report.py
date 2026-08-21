"""Live: seed → engine (resolve in-process) → ClickHouse, then pin BOTH deliverables
against the frozen tiny fixture. Skipped when services are unreachable, so
`make test` stays green offline; runs under `make test-int` (CI from Phase 3).

- eval: household-grain precision 0.673 (35/52), recall 1.000 (35/35), scored
  from ClickHouse FINAL joined against the truth side file in the harness.
- report: the per-campaign metric grid, VALUES pinned (a wrong-ratio SQL bug
  that still used FINAL + nullIf would pass a shape-only check).
"""

import json
import os
from pathlib import Path

import pytest
from confluent_kafka import Consumer

from accuracy.score import score
from clickhouse.client import connect, read_credited, read_exposure_households
from producer.seed import main as seed_main
from queries.report import run as run_report
from streaming.dataflow import run_engine
from tests.pins import TINY_HOT

BROKER = os.environ.get("KAFKA_BROKER", "127.0.0.1:19092")
REPO_ROOT = Path(__file__).parent.parent.parent
TRUTH = REPO_ROOT / "data" / "truth" / "tiny" / "truth_links.jsonl"

# per-campaign pins (DECISIONS Phase 4 / specs/phase-4.md); FINAL-deduped
# denominators (150 exposures, not the 164 raw rows)
EXPECTED_REPORT = {
    #            spend  exps conv pur revenue  roas      cpa    cvr     svr
    "camp-00": (4.48, 47, 16, 7, 431.99, 96.4263, 0.6400, 0.3404, 0.1915),
    "camp-01": (4.86, 56, 24, 9, 626.71, 128.9527, 0.5400, 0.4286, 0.2679),
    "camp-02": (4.36, 47, 12, 5, 324.14, 74.3440, 0.8720, 0.2553, 0.1489),
}


@pytest.fixture(scope="module", autouse=True)
def _require_services() -> None:
    consumer = Consumer({"bootstrap.servers": BROKER, "group.id": "eval-int-probe"})
    try:
        consumer.list_topics(timeout=3)
    except Exception:
        pytest.skip("broker unreachable — run `make up` for integration tests")
    finally:
        consumer.close()
    try:
        connect().command("select 1")
    except Exception:
        pytest.skip("clickhouse unreachable — run `make up` for integration tests")


@pytest.fixture(scope="module", autouse=True)
def _populate() -> None:
    seed_main(["--profile", "tiny"])
    run_engine(BROKER)


def _load_truth() -> dict[str, str]:
    return {
        json.loads(line)["conversion_id"]: json.loads(line)["truth_exposure_id"]
        for line in TRUTH.read_text().splitlines()
    }


# Both tests read GLOBAL FINAL table state (whole exposures_landed /
# attributed_conversions), so the pins assume a tiny-only stack. When Phase 5
# adds `medium`, seeding it into the same ClickHouse would break these pins
# through no fault of the SQL — isolate by profile (or a fresh DB) then.
def test_eval_reproduces_pinned_accuracy() -> None:
    client = connect()
    report = score(
        read_credited(client), _load_truth(), read_exposure_households(client)
    )
    assert (report.credited, report.truth_links, report.household_correct) == (
        TINY_HOT.credited,
        TINY_HOT.truth,
        TINY_HOT.correct,
    )
    assert report.precision == pytest.approx(TINY_HOT.precision)
    assert report.recall == pytest.approx(TINY_HOT.recall)
    assert report.organic_credited == 17
    assert report.exact_exposure_correct == 3


def test_report_reproduces_pinned_grid() -> None:
    rows = {r[0]: r for r in run_report(connect())}
    assert set(rows) == set(EXPECTED_REPORT)
    for campaign, expected in EXPECTED_REPORT.items():
        _, spend, exps, conv, pur, revenue, roas, cpa, cvr, svr = rows[campaign]
        assert spend == pytest.approx(expected[0], abs=0.01)
        assert exps == expected[1]
        assert conv == expected[2]
        assert pur == expected[3]
        assert revenue == pytest.approx(expected[4], abs=0.01)
        assert roas == pytest.approx(expected[5], abs=0.001)
        assert cpa == pytest.approx(expected[6], abs=0.001)
        assert cvr == pytest.approx(expected[7], abs=0.001)
        assert svr == pytest.approx(expected[8], abs=0.001)
