"""One test per producer knob, plus determinism of the whole stream."""

from typing import Any

from producer.config import Profile, load_profile
from producer.generate import ON_TIME_MAX_JITTER_S, generate
from producer.serialize import jsonl


def profile(**event_overrides: Any) -> Profile:
    base = load_profile("tiny").model_dump()
    # Larger stream than tiny so knob effects are statistically visible.
    base["graph"]["n_households"] = 50
    defaults = dict(
        n_exposures=1000,
        late={"fraction": 0.0, "min_minutes": 0, "max_minutes": 0},
        duplicate_fraction=0.0,
        unknown_device_fraction=0.0,
        co_view_multiplier={},
    )
    base["events"].update({**defaults, **event_overrides})
    return Profile.model_validate(base)


def test_same_seed_byte_identical_different_seed_not() -> None:
    a, b, c = generate(profile(), 42), generate(profile(), 42), generate(profile(), 43)
    for field in ("exposures", "conversions", "truth_links"):
        assert jsonl(getattr(a, field)) == jsonl(getattr(b, field))
    assert jsonl(a.exposures) != jsonl(c.exposures)


def test_throughput_knob_sets_event_time_spacing() -> None:
    stream = generate(profile(events_per_hour=60), 1)
    originals = {e.exposure_id: e for e in stream.exposures}
    times = [
        e.event_time for e in sorted(originals.values(), key=lambda e: e.exposure_id)
    ]
    deltas = {(b - a).total_seconds() for a, b in zip(times, times[1:], strict=False)}
    assert deltas == {60.0}


def test_late_injector_fraction_and_bounds() -> None:
    stream = generate(
        profile(late={"fraction": 0.3, "min_minutes": 30, "max_minutes": 180}), 1
    )
    events = list({e.exposure_id: e for e in stream.exposures}.values())
    lateness = [(e.ingest_time - e.event_time).total_seconds() for e in events]
    late = [s for s in lateness if s > ON_TIME_MAX_JITTER_S]
    assert all(30 * 60 <= s <= 180 * 60 for s in late)
    assert 0.2 < len(late) / len(events) < 0.4
    assert all(0 < s <= ON_TIME_MAX_JITTER_S for s in lateness if s not in late)


def test_caused_conversion_rate_scales_truth_links() -> None:
    low = generate(profile(caused_conversion_rate=0.1), 1)
    high = generate(profile(caused_conversion_rate=0.4), 1)
    zero = generate(profile(caused_conversion_rate=0.0, organic_conversions=0), 1)
    assert len(high.truth_links) > 2 * len(low.truth_links)
    assert not zero.truth_links and not zero.conversions


def test_duplicate_injector_reemits_identical_payloads() -> None:
    stream = generate(profile(duplicate_fraction=0.2), 1)
    by_id: dict[str, list] = {}
    for e in stream.exposures:
        by_id.setdefault(e.exposure_id, []).append(e)
    dup_ids = [i for i, es in by_id.items() if len(es) > 1]
    assert 0.1 < len(dup_ids) / len(by_id) < 0.3
    for i in dup_ids:
        assert len(by_id[i]) == 2
        assert by_id[i][0] == by_id[i][1]  # byte-identical re-send, same ingest_time


def test_no_duplicates_when_fraction_zero() -> None:
    stream = generate(profile(duplicate_fraction=0.0), 1)
    ids = [e.exposure_id for e in stream.exposures]
    assert len(ids) == len(set(ids))


def test_unknown_device_fraction_bounds() -> None:
    for fraction, expect_known in [(0.0, True), (1.0, False)]:
        stream = generate(profile(unknown_device_fraction=fraction), 1)
        graph_devices = {
            d.device_id for h in stream.graph.households for d in h.devices
        }
        in_graph = [c.device_id in graph_devices for c in stream.conversions]
        assert all(in_graph) if expect_known else not any(in_graph)

    mixed = generate(profile(unknown_device_fraction=0.3), 1)
    unknown = [c for c in mixed.conversions if c.device_id.startswith("u-")]
    assert unknown, "expected some unknown-device conversions"
    # An unknown device's IP must come from the TRUE (causing) household —
    # that invariant is what keeps shared IPs the sole source of
    # wrong-household matches. Checkable for caused conversions via truth.
    household_ips = {h.household_id: set(h.ips) for h in mixed.graph.households}
    exposures = {e.exposure_id: e for e in mixed.exposures}
    causing = {t.conversion_id: t.truth_exposure_id for t in mixed.truth_links}
    caused_unknown = [c for c in unknown if c.conversion_id in causing]
    assert caused_unknown, "expected caused unknown-device conversions"
    for c in caused_unknown:
        true_household = exposures[causing[c.conversion_id]].household_id
        assert c.ip in household_ips[true_household]


def test_co_view_multiplier_scales_caused_conversions_per_genre() -> None:
    stream = generate(
        profile(co_view_multiplier={"sports": 2.0}, caused_conversion_rate=0.2), 1
    )
    exposures = {e.exposure_id: e for e in stream.exposures}
    caused_by_genre = {"sports": 0, "news": 0}
    seen_by_genre = {"sports": 0, "news": 0}
    for e in exposures.values():
        if e.program_genre in seen_by_genre:
            seen_by_genre[e.program_genre] += 1
    caused_exposures = {t.truth_exposure_id for t in stream.truth_links}
    for eid in caused_exposures:
        genre = exposures[eid].program_genre
        if genre in caused_by_genre:
            caused_by_genre[genre] += 1
    sports_rate = caused_by_genre["sports"] / seen_by_genre["sports"]
    news_rate = caused_by_genre["news"] / seen_by_genre["news"]
    assert sports_rate > 1.4 * news_rate


def test_truth_links_reference_real_events_and_same_household() -> None:
    # Run on both a no-unknown-devices stream and the real tiny profile:
    # device-hit conversions must be on a device of the causing household;
    # unknown-device conversions must at least carry a causing-household IP.
    for stream in [generate(profile(), 42), generate(load_profile("tiny"), 42)]:
        exposures = {e.exposure_id: e for e in stream.exposures}
        conversions = {c.conversion_id: c for c in stream.conversions}
        device_household = {
            d.device_id: h.household_id
            for h in stream.graph.households
            for d in h.devices
        }
        household_ips = {h.household_id: set(h.ips) for h in stream.graph.households}
        assert stream.truth_links, "expected caused conversions"
        for link in stream.truth_links:
            exposure = exposures[link.truth_exposure_id]
            conversion = conversions[link.conversion_id]
            if conversion.device_id in device_household:
                assert device_household[conversion.device_id] == exposure.household_id
            else:
                assert conversion.ip in household_ips[exposure.household_id]
            assert conversion.event_time > exposure.event_time


def test_emit_order_is_arrival_order() -> None:
    stream = generate(
        profile(late={"fraction": 0.3, "min_minutes": 30, "max_minutes": 180}), 1
    )
    ingest_times = [e.ingest_time for e in stream.exposures]
    assert ingest_times == sorted(ingest_times)


def test_emit_order_with_duplicates() -> None:
    # A duplicate carries the original ingest_time but arrives later, so the
    # raw ingest sequence is non-monotonic; originals (first occurrence of
    # each id) must still be in arrival order, and each duplicate must come
    # after its original.
    stream = generate(
        profile(
            duplicate_fraction=0.2,
            late={"fraction": 0.3, "min_minutes": 30, "max_minutes": 180},
        ),
        1,
    )
    first_seen: dict[str, int] = {}
    for idx, e in enumerate(stream.exposures):
        if e.exposure_id in first_seen:
            assert idx > first_seen[e.exposure_id]
        else:
            first_seen[e.exposure_id] = idx
    originals = sorted(first_seen.items(), key=lambda kv: kv[1])
    by_id = {e.exposure_id: e for e in stream.exposures}
    ingest_times = [by_id[eid].ingest_time for eid, _ in originals]
    assert ingest_times == sorted(ingest_times)
