"""Seeded device-graph generator: households → devices + IPs."""

import random

from producer.config import GraphConfig
from producer.models import Device, DeviceGraph, Household

PERSONAL_KINDS = ["phone", "laptop", "tablet"]


def build_graph(cfg: GraphConfig, rng: random.Random) -> DeviceGraph:
    # Shared IPs come from a small CGNAT-style pool so several households can
    # collide on one address. Sized to the expected number of shared slots so
    # multiple distinct shared IPs exist even at tiny scale.
    pool_size = max(1, round(cfg.n_households * cfg.shared_ip_fraction))
    shared_pool = [f"100.64.0.{i + 1}" for i in range(pool_size)]
    unique_counter = 0

    households: list[Household] = []
    for i in range(cfg.n_households):
        household_id = f"h-{i:04d}"
        n_devices = rng.randint(*cfg.devices_per_household)
        devices = [
            Device(
                device_id=f"d-{i:04d}-{j}",
                kind="tv" if j == 0 else rng.choice(PERSONAL_KINDS),
            )
            for j in range(n_devices)
        ]
        ips: list[str] = []
        for _ in range(rng.randint(*cfg.ips_per_household)):
            if cfg.shared_ip_fraction > 0 and rng.random() < cfg.shared_ip_fraction:
                ip = rng.choice(shared_pool)
            else:
                unique_counter += 1
                octets = (
                    unique_counter // 65536 % 256,
                    unique_counter // 256 % 256,
                    unique_counter % 256,
                )
                ip = "10.{}.{}.{}".format(*octets)
            if ip not in ips:
                ips.append(ip)
        households.append(
            Household(household_id=household_id, devices=devices, ips=ips)
        )
    return DeviceGraph(households=households)
