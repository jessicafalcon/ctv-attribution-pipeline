"""Push a batch stage's terminal Prometheus registry to the Pushgateway (Phase 18b).

The pipeline stages are finite drains — they exit before Prometheus can pull-scrape — so
each pushes its terminal registry to the Pushgateway, which PERSISTS it for Prometheus
to scrape and the alert rules to evaluate on. This closes the "batch stages exit before
a scrape" gap every prior phase deferred.

Gated on `PUSHGATEWAY_URL`: unset (the golden / oracle / capture / offline paths, and CI
unit runs) → every function here is a no-op, so nothing in those paths depends on a
running gateway. `make run` sets it and resets the gateway once at the start.

`python -m observability.push --reset` wipes all pushed groups, so a stage that does not
run this pass leaves no stale metric behind (per-run reset).
"""

import argparse
import os
import urllib.request

from prometheus_client import CollectorRegistry, push_to_gateway


def gateway_url() -> str | None:
    return os.environ.get("PUSHGATEWAY_URL") or None


def push_registry(registry: CollectorRegistry, job: str) -> None:
    """Push `registry` under grouping {job}. No-op if PUSHGATEWAY_URL is unset.
    `push_to_gateway` REPLACES the job's group, so a re-run overwrites rather than
    accumulates."""
    url = gateway_url()
    if url is None:
        return
    push_to_gateway(url, job=job, registry=registry)


def reset_gateway() -> None:
    """Wipe every pushed group (Pushgateway admin API). No-op if PUSHGATEWAY_URL unset.
    Part of `make run`, so a stage that skips this pass leaves no stale metric."""
    url = gateway_url()
    if url is None:
        return
    req = urllib.request.Request(f"{url}/api/v1/admin/wipe", method="PUT")
    urllib.request.urlopen(req, timeout=10)


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--reset", action="store_true", help="wipe all pushed groups")
    args = parser.parse_args(argv)
    if args.reset:
        reset_gateway()


if __name__ == "__main__":
    main()
