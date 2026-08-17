import random

from producer.config import GraphConfig
from producer.graph import build_graph


def cfg(**overrides: object) -> GraphConfig:
    base = dict(
        n_households=50,
        devices_per_household=(2, 4),
        ips_per_household=(1, 2),
        shared_ip_fraction=0.2,
    )
    return GraphConfig(**{**base, **overrides})


def test_counts_and_shape() -> None:
    graph = build_graph(cfg(), random.Random(1))
    assert len(graph.households) == 50
    for h in graph.households:
        assert 2 <= len(h.devices) <= 4
        assert 1 <= len(h.ips) <= 2
        assert h.devices[0].kind == "tv"
        assert len({d.device_id for d in h.devices}) == len(h.devices)


def test_same_seed_identical_different_seed_not() -> None:
    a = build_graph(cfg(), random.Random(7))
    b = build_graph(cfg(), random.Random(7))
    c = build_graph(cfg(), random.Random(8))
    assert a == b
    assert a != c


def test_shared_ip_fraction_zero_means_no_shared_ips() -> None:
    graph = build_graph(cfg(shared_ip_fraction=0.0), random.Random(1))
    owners: dict[str, int] = {}
    for h in graph.households:
        for ip in h.ips:
            owners[ip] = owners.get(ip, 0) + 1
    assert all(count == 1 for count in owners.values())


def test_shared_ip_fraction_produces_cross_household_ips() -> None:
    graph = build_graph(cfg(n_households=200, shared_ip_fraction=0.3), random.Random(1))
    owners: dict[str, set[str]] = {}
    for h in graph.households:
        for ip in h.ips:
            owners.setdefault(ip, set()).add(h.household_id)
    shared = [ip for ip, hs in owners.items() if len(hs) > 1]
    assert shared, "expected at least one IP shared across households"
    assert all(ip.startswith("100.64.") for ip in shared)
