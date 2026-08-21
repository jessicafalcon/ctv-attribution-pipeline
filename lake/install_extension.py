"""Install the DuckDB `iceberg` extension once, at setup (Phase 12).

The extension is a signed binary DuckDB fetches from extensions.duckdb.org, tied to
the duckdb version — the ONE Phase-12 dependency NOT hash-pinned in uv.lock
(DECISIONS Phase 12). `make setup` and the CI offline job run this once (network is
allowed there); every test and the reconcile read then does `load iceberg` only, so
the offline unit suite (`make test`) never reaches for the network and fails loud
if setup was skipped, rather than silently fetching.

Run: `python -m lake.install_extension` (idempotent — a no-op once cached on disk).
"""

import duckdb


def install_iceberg() -> None:
    duckdb.connect().execute("install iceberg")


if __name__ == "__main__":
    install_iceberg()
    print("duckdb iceberg extension installed (load-only from here on)")
