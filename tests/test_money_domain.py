"""Money is quantized to CENTS by the producer - the premise the rollup's exact
Decimal path rests on (reconcile/rollup.py; RUNBOOK incident 3).

`toDecimal64(toString(x), 4)` is exact for values with <= 2 decimals; that is
only a guarantee if the producer never emits a 3-dp value. The quantization
lives at producer/generate.py (`spend=round(rng.uniform(...), 2)` and
`revenue=round(rng.uniform(...), 2)`). This pins it: every spend/revenue in the
frozen tiny fixtures and in a fresh generation of every profile has <= 2
decimals. If the generator ever emits a 3-dp value, this fails loud instead of
the rollup silently truncating again.
"""

import json
from decimal import Decimal
from pathlib import Path

import pytest

from producer import generate as gen
from producer.config import PROFILES_DIR, load_profile

FIXTURES = Path(__file__).parent.parent / "fixtures" / "tiny"
PROFILES = sorted(p.stem for p in PROFILES_DIR.glob("*.json"))


def _decimals(x: float) -> int:
    return max(0, -Decimal(str(x)).as_tuple().exponent)


def test_frozen_tiny_fixture_money_has_at_most_two_decimals() -> None:
    for name, key in (("exposures.jsonl", "spend"), ("conversions.jsonl", "revenue")):
        values = [
            json.loads(ln)[key] for ln in (FIXTURES / name).read_text().splitlines()
        ]
        assert values and all(_decimals(v) <= 2 for v in values), name


CENT_DOMAIN = (0.0, 999.99)  # the domain the live exhaustive pin proves exact


def _money_ranges(obj: object) -> list[tuple[str, tuple[float, float]]]:
    """Every spend_range / revenue_range anywhere in the profile (events nest)."""
    found: list[tuple[str, tuple[float, float]]] = []
    if isinstance(obj, dict):
        for k, v in obj.items():
            if k in ("spend_range", "revenue_range"):
                found.append((k, tuple(v)))
            else:
                found += _money_ranges(v)
    elif isinstance(obj, list):
        for v in obj:
            found += _money_ranges(v)
    return found


@pytest.mark.parametrize("name", PROFILES)
def test_every_profile_generates_cent_quantized_money(name: str) -> None:
    # A SMALL generation per profile: the rounding is per draw (producer/
    # generate.py `round(rng.uniform(…), 2)`), not per count, so bench_large's
    # 55k exposures prove nothing 200 do not — and the unit suite runs on every
    # edit.
    base = load_profile(name)
    # n_exposures lives on Profile.events; model_copy does not validate, so an
    # update on the wrong level would silently do nothing (the repo idiom:
    # streaming/scale_probe.py)
    p = base.model_copy(
        update={"events": base.events.model_copy(update={"n_exposures": 200})}
    )
    assert p.events.n_exposures == 200
    s = gen.generate(p, p.seed)
    assert s.exposures and all(_decimals(e.spend) <= 2 for e in s.exposures), name
    assert all(_decimals(c.revenue) <= 2 for c in s.conversions), name


@pytest.mark.parametrize("name", PROFILES)
def test_every_profile_money_range_sits_inside_the_proven_cent_domain(
    name: str,
) -> None:
    # The live exhaustive pin proves the toString→Decimal path exact over
    # CENT_DOMAIN. A future profile with a $5,000 SKU must fail HERE, not leave the
    # proven domain silently (toString of a large Float64 may also switch to
    # scientific notation, which toDecimal64 may not parse).
    ranges = _money_ranges(load_profile(name).model_dump())
    assert ranges, name
    for key, (lo, hi) in ranges:
        assert CENT_DOMAIN[0] <= lo <= hi <= CENT_DOMAIN[1], f"{name}: {key}={lo, hi}"
