from dbt.adapters.base import AdapterPlugin

from dbt.adapters.hotdata.connections import HotdataConnectionManager
from dbt.adapters.hotdata.credentials import HotdataCredentials
from dbt.adapters.hotdata.impl import HotdataAdapter
from dbt.include import hotdata

Plugin = AdapterPlugin(
    adapter=HotdataAdapter,  # type: ignore[arg-type]
    credentials=HotdataCredentials,
    include_path=hotdata.PACKAGE_PATH,
)

__all__ = [
    "HotdataAdapter",
    "HotdataConnectionManager",
    "HotdataCredentials",
    "Plugin",
]
