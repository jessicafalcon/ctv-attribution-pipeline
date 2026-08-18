"""Lookup indexes over the device graph: device_id → household, ip → owners.

Built from Household records — either the frozen fixture / mirror jsonl or the
compacted `device_graph` topic. A compacted topic keeps only the last message
per key, so consuming it to the end reconstructs the current graph; we apply
last-write-wins per household_id to match that semantics regardless of source.
"""

from collections.abc import Iterable

from producer.models import Household


class GraphIndex:
    """Immutable-after-build lookup. `device_of` maps a device to its household;
    `owners_of` maps an IP to every household that lists it (≥2 = shared IP,
    the sole source of ambiguous fan-out)."""

    def __init__(self, device_of: dict[str, str], owners_of: dict[str, list[str]]):
        self.device_of = device_of
        self.owners_of = owners_of

    @classmethod
    def from_households(cls, households: Iterable[Household]) -> "GraphIndex":
        # Last write per household_id wins (compacted-topic semantics).
        latest: dict[str, Household] = {h.household_id: h for h in households}
        device_of: dict[str, str] = {}
        owners: dict[str, set[str]] = {}
        for hid, h in latest.items():
            for d in h.devices:
                device_of[d.device_id] = hid
            for ip in h.ips:
                owners.setdefault(ip, set()).add(hid)
        # Sort owners so ambiguous fan-out is emitted in a deterministic order.
        owners_of = {ip: sorted(hids) for ip, hids in owners.items()}
        return cls(device_of, owners_of)
