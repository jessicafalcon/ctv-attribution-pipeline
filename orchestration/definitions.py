"""Dagster Definitions — the code location the webserver and CLI load.

`dagster dev -m orchestration.definitions` serves the asset graph UI; the headless
make target uses orchestration.run instead (no server needed).
"""

from dagster import Definitions

from orchestration.assets import (
    exposures_iceberg,
    reconciled_conversions,
    reconciled_report,
)

defs = Definitions(
    assets=[exposures_iceberg, reconciled_conversions, reconciled_report]
)
