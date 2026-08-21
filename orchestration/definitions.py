"""Dagster Definitions — the code location the webserver and CLI load.

`dagster dev -m orchestration.definitions` serves the asset graph UI; the headless
make target uses orchestration.run instead (no server needed).
"""

from dagster import Definitions

from orchestration.assets import (
    attributed_iceberg,
    clickhouse_attributed_conversions,
    clickhouse_exposures_landed,
    exposures_iceberg,
    reconciled_conversions,
    reconciled_report,
)

defs = Definitions(
    assets=[
        exposures_iceberg,
        attributed_iceberg,
        clickhouse_exposures_landed,
        clickhouse_attributed_conversions,
        reconciled_conversions,
        reconciled_report,
    ]
)
