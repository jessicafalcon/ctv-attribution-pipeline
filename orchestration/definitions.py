"""Dagster Definitions — the code location the webserver loads.

`make dagster-ui` = `DAGSTER_PROFILE=<p> dagster dev -m orchestration.definitions`:
the asset-graph viewer, with materialize working for the ONE profile bound here
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
from orchestration.maintenance import lake_maintenance

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
    jobs=[lake_maintenance],
)
