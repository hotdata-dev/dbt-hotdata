from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any, ClassVar

from dbt.adapters.contracts.connection import Credentials
from dbt.adapters.events.logging import AdapterLogger
from dbt_common.exceptions import DbtRuntimeError
from hotdata_framework.env import (
    default_api_key,
    default_host,
    explicit_workspace_id,
    normalize_host,
)

logger = AdapterLogger("Hotdata")


@dataclass
class HotdataCredentials(Credentials):
    """Profile (`profiles.yml`) fields for the `hotdata` adapter.

    Addressing follows the Hotdata id-first convention: an instant database is
    identified by its **id**, never by name (names are not unique). With no
    ``database_id`` and ``create_database_if_missing`` (the default), the first
    run creates a database labelled ``database_name`` and logs its id — pin
    that id in the profile so later runs keep building into the same database.

    The API key is a secret: it is read from ``HOTDATA_API_KEY`` when not set
    in the profile (where `env_var()` is the recommended way to set it). The
    workspace id is routing, not a credential — it is a plain profile field.

    Ambient-environment fallbacks: fields left unset in the profile resolve
    from the platform's own ``HOTDATA_*`` environment variables
    (``HOTDATA_API_KEY``, ``HOTDATA_WORKSPACE``, ``HOTDATA_DATABASE``,
    ``HOTDATA_API_URL``), so the same project runs unchanged under any
    orchestrator that sets them, such as hotdata-dlt-destination's dbt bridge.
    Explicit profile values always win over the environment. Resolution reuses
    the ``hotdata_framework.env`` helpers, so URL normalization and variable
    names stay identical across every SDK consumer.
    """

    # dbt's relation namespace. Inside an instant database the SQL catalog is
    # always literally "default" ("default"."<schema>"."<table>"), so this stays
    # fixed; the instant database itself is selected by database_id below.
    database: str = "default"
    schema: str = "public"

    workspace_id: str | None = None
    api_key: str | None = None
    database_id: str | None = None
    database_name: str = "dbt"
    create_database_if_missing: bool = True
    # None means "unset": resolved in __post_init__ via default_host(), which
    # reads HOTDATA_API_URL and falls back to the platform default. An explicit
    # value always wins over the environment.
    api_base_url: str | None = None
    # Loads take a catalog-level lock per database, so a concurrent writer can
    # hold 409s for tens of seconds — the budget must outlast that, not just
    # blips. 8 attempts x 1.5s linear backoff ~= 42s.
    max_retries: int = 8
    retry_backoff_seconds: float = 1.5

    _ALIASES: ClassVar[dict[str, str]] = {"host": "api_base_url", "token": "api_key"}

    @property
    def type(self) -> str:
        return "hotdata"

    @property
    def unique_field(self) -> str:
        return self.workspace_id or self.api_base_url or default_host()

    def _connection_keys(self) -> tuple[str, ...]:
        # api_key deliberately omitted: these are echoed by `dbt debug`.
        # `database` must be present — dbt builds the `target` jinja dict from
        # these keys, and generate_database_name() reads target.database; when
        # missing, every node's database renders as '' and refs compile to
        # ""."schema"."table".
        return (
            "database",
            "workspace_id",
            "database_id",
            "database_name",
            "schema",
            "api_base_url",
            "create_database_if_missing",
            "max_retries",
            "retry_backoff_seconds",
        )

    def _api_key_or_none(self) -> str | None:
        return self.api_key or default_api_key() or None

    def resolve_api_key(self) -> str:
        api_key = self._api_key_or_none()
        if not api_key:
            raise DbtRuntimeError(
                "hotdata profile is missing the API key: set HOTDATA_API_KEY in the "
                "environment, or put api_key: \"{{ env_var('HOTDATA_API_KEY') }}\" in "
                "profiles.yml."
            )
        return api_key

    # NB: not named `validate` — dbtClassMixin.validate(data) is a schema-check
    # classmethod dbt calls while loading profiles; shadowing it breaks parsing.
    def validate_connection_setup(self) -> None:
        """Fail at connection time naming the missing field, not mid-run."""
        missing = []
        if not self._api_key_or_none():
            missing.append("api_key (set HOTDATA_API_KEY or api_key: in profiles.yml)")
        if not self.workspace_id:
            missing.append(
                "workspace_id (set workspace_id: in profiles.yml, or HOTDATA_WORKSPACE "
                "in the environment)"
            )
        if missing:
            raise DbtRuntimeError(
                "hotdata profile is missing required configuration: " + "; ".join(missing)
            )

    def __post_init__(self) -> None:
        if self.database and self.database != "default":
            raise DbtRuntimeError(
                f"hotdata profile sets database={self.database!r}, but the SQL catalog "
                "inside a Hotdata instant database is always 'default'. Select the "
                "instant database with database_id: instead, and leave database: unset."
            )
        # Ambient-environment fallbacks (see class docstring). Only fields the
        # profile leaves unset resolve from the environment; api_key stays lazy
        # in resolve_api_key() so the secret is never stored or echoed here.
        if not self.workspace_id:
            self.workspace_id = explicit_workspace_id() or None
        if not self.database_id:
            self.database_id = os.environ.get("HOTDATA_DATABASE") or None
            if self.database_id:
                # Retargeting the whole build must be visible: without this
                # line, a leftover shell export silently redirects loads into
                # an existing database.
                logger.info(
                    f"database_id {self.database_id} taken from the HOTDATA_DATABASE "
                    "environment variable (no database_id: in the profile)"
                )
        if self.api_base_url:
            self.api_base_url = normalize_host(self.api_base_url)
        else:
            self.api_base_url = default_host()


def credentials_repr(credentials: HotdataCredentials) -> dict[str, Any]:
    """Loggable summary (never includes the API key)."""
    return dict(credentials.connection_info())
