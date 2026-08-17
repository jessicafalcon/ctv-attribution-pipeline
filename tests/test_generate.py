"""One test per producer knob, plus determinism of the whole stream."""

from producer.config import Profile, load_profile
from producer.generate import generate
from producer.serialize import jsonl


def profile(**event_overrides) -> Profile:
    base = load_profile("tiny").model_dump()
    # Larger stream than tiny so knob effects are statistically visible.
    base["graph"]["n_households"] = 50
    defaults = dict(
        n_exposures=1000,
        late={"fraction": 0.0, "min_minutes": 0, "max_minutes": 0},
        duplicate_fraction=0.0,
        co_view_multiplier={},
    )
    base["events"].update({**defaults, **event_overrides})
    return Profile.model_validate(base)


def test_same_seed_byte_identical_different_seed_not():
    a, b, c = generate(profile(), 42), generate(profile(), 42), generate(profile(), 43)
    for field in ("exposures", "conversions", "truth_links"):
        assert jsonl(getattr(a, field)) == jsonl(getattr(b, field))
    assert jsonl(a.exposures) != jsonl(c.exposures)


def test_throughput_knob_sets_event_time_spacing():
    stream = generate(profile(events_per_hour=60), 1)
    originals = {e.exposure_id: e for e in stream.exposures}
    times = [
        e.event_time for e in sorted(originals.values(), key=lambda e: e.exposure_id)
    ]
    deltas = {(b - a).total_seconds() for a, b in zip(times, times[1:], strict=False)}
    assert deltas == {60.0}


def test_late_injector_fraction_and_bounds():
    stream = generate(
        profile(late={"fraction": 0.3, "min_minutes": 30, "max_minutes": 180}), 1
    )
    events = list({e.exposure_id: e for e in stream.exposures}.values())
    lateness = [(e.ingest_time - e.event_time).total_seconds() for e in events]
    late = [s for s in lateness if s > 60]
    assert all(30 * 60 <= s <= 180 * 60 for s in late)
    assert 0.2 < len(late) / len(events) < 0.4
    assert all(0 < s <= 60 for s in lateness if s <= 60)


def test_duplicate_injector_reemits_identical_payloads():
    stream = generate(profile(duplicate_fraction=0.2), 1)
    by_id: dict[str, list] = {}
    for e in stream.exposures:
        by_id.setdefault(e.exposure_id, []).append(e)
    dup_ids = [i for i, es in by_id.items() if len(es) > 1]
    assert 0.1 < len(dup_ids) / len(by_id) < 0.3
    for i in dup_ids:
        assert len(by_id[i]) == 2
        assert by_id[i][0] == by_id[i][1]  # byte-identical re-send, same ingest_time


def test_no_duplicates_when_fraction_zero():
    stream = generate(profile(duplicate_fraction=0.0), 1)
    ids = [e.exposure_id for e in stream.exposures]
    assert len(ids) == len(set(ids))


def test_co_view_multiplier_scales_caused_conversions_per_genre():
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


def test_truth_links_reference_real_events_and_same_household():
    stream = generate(profile(), 42)
    exposures = {e.exposure_id: e for e in stream.exposures}
    conversions = {c.conversion_id: c for c in stream.conversions}
    device_household = {
        d.device_id: h.household_id for h in stream.graph.households for d in h.devices
    }
    assert stream.truth_links, "expected caused conversions"
    for link in stream.truth_links:
        exposure = exposures[link.truth_exposure_id]
        conversion = conversions[link.conversion_id]
        assert device_household[conversion.device_id] == exposure.household_id
        assert conversion.event_time > exposure.event_time


def test_emit_order_is_arrival_order():
    stream = generate(
        profile(late={"fraction": 0.3, "min_minutes": 30, "max_minutes": 180}), 1
    )
    ingest_times = [e.ingest_time for e in stream.exposures]
    assert ingest_times == sorted(ingest_times)
