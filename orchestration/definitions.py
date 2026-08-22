"""Dagster Definitions — the code location the webserver loads.

`make dagster-ui` = `DAGSTER_PROFILE=<p> dagster dev -m orchestration.definitions`:
the asset-graph viewer, with materialize of the non-destructive assets working for
the ONE profile bound here
(there is no default lake root — an unbound code location renders the graph but
every asset fails loud with `LakeRootUnset`). The headless CLI
(`orchestration.run`) binds its own profile from `--profile` and never loads this.
"""

import os

from dagster import Definitions

from lake.iceberg_catalog import configure
from orchestration.assets import (
    attributed_iceberg,
    clickhouse_attributed_conversions,
    clickhouse_exposures_landed,
    exposures_iceberg,
    reconciled_conversions,
    reconciled_report,
)

if os.environ.get("DAGSTER_PROFILE"):
    configure(os.environ["DAGSTER_PROFILE"])

defs = Definitions(
    assets=[
        exposures_iceberg,
        attributed_iceberg,
        clickhouse_exposures_landed,
        clickhouse_attributed_conversions,
        reconciled_conversions,
        reconciled_report,
    ],
    # No jobs: `lake_maintenance` rewrites the record's data files — destructive
    # by this repo's definition — and has exactly ONE entry point, the prompting
    # `lake.destructive maintain` (review gate, round 5). The UI is a viewer plus
    # a profile-bound materialize of the NON-destructive assets.
)
