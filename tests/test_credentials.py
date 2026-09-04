from __future__ import annotations

import pytest
from dbt_common.exceptions import DbtRuntimeError

from dbt.adapters.hotdata.credentials import HotdataCredentials


def _creds(**kwargs) -> HotdataCredentials:
    from typing import Any

    base: dict[str, Any] = {"database": "default", "schema": "public"}
    base.update(kwargs)
    return HotdataCredentials(**base)


def test_type_and_unique_field():
    creds = _creds(workspace_id="ws_1")
    assert creds.type == "hotdata"
    assert creds.unique_field == "ws_1"


def test_connection_info_never_echoes_api_key():
    creds = _creds(workspace_id="ws_1", api_key="hk_secret")
    info = dict(creds.connection_info())
    assert "api_key" not in info
    assert "hk_secret" not in str(info)
    assert info["workspace_id"] == "ws_1"


def test_validate_names_every_missing_field():
    creds = _creds()
    with pytest.raises(DbtRuntimeError) as excinfo:
        creds.validate_connection_setup()
    message = str(excinfo.value)
    assert "api_key" in message
    assert "HOTDATA_API_KEY" in message
    assert "workspace_id" in message


def test_api_key_falls_back_to_environment(monkeypatch):
    monkeypatch.setenv("HOTDATA_API_KEY", "hk_env")
    creds = _creds(workspace_id="ws_1")
    creds.validate_connection_setup()
    assert creds.resolve_api_key() == "hk_env"


def test_profile_api_key_beats_environment(monkeypatch):
    monkeypatch.setenv("HOTDATA_API_KEY", "hk_env")
    creds = _creds(workspace_id="ws_1", api_key="hk_profile")
    assert creds.resolve_api_key() == "hk_profile"


def test_non_default_database_is_rejected_up_front():
    with pytest.raises(DbtRuntimeError) as excinfo:
        _creds(database="analytics", workspace_id="ws_1")
    assert "database_id" in str(excinfo.value)


def test_defaults():
    creds = _creds(workspace_id="ws_1")
    assert creds.database == "default"
    assert creds.schema == "public"
    assert creds.database_name == "dbt"
    assert creds.create_database_if_missing is True
    assert creds.api_base_url == "https://api.hotdata.dev"
    assert creds.max_retries == 8


# --- ambient-environment fallbacks (hotdata CLI scoped env, orchestrator bridges) --


def test_workspace_and_database_fall_back_to_hotdata_env(monkeypatch):
    monkeypatch.setenv("HOTDATA_WORKSPACE", "ws_scoped")
    monkeypatch.setenv("HOTDATA_DATABASE", "db_scoped")
    creds = _creds()
    assert creds.workspace_id == "ws_scoped"
    assert creds.database_id == "db_scoped"


def test_profile_values_beat_env(monkeypatch):
    monkeypatch.setenv("HOTDATA_WORKSPACE", "ws_scoped")
    monkeypatch.setenv("HOTDATA_DATABASE", "db_scoped")
    creds = _creds(workspace_id="ws_profile", database_id="db_profile")
    assert creds.workspace_id == "ws_profile"
    assert creds.database_id == "db_profile"


def test_api_base_url_falls_back_to_env(monkeypatch):
    monkeypatch.setenv("HOTDATA_API_URL", "https://staging.hotdata.dev")
    assert _creds().api_base_url == "https://staging.hotdata.dev"
    explicit = _creds(api_base_url="https://eu.hotdata.dev")
    assert explicit.api_base_url == "https://eu.hotdata.dev"


def test_api_base_url_pinned_to_platform_default_beats_env(monkeypatch):
    monkeypatch.setenv("HOTDATA_API_URL", "https://staging.hotdata.dev")
    pinned = _creds(api_base_url="https://api.hotdata.dev")
    assert pinned.api_base_url == "https://api.hotdata.dev"


def test_api_base_url_is_normalized():
    assert (
        _creds(api_base_url="https://eu.hotdata.dev/v1/").api_base_url == "https://eu.hotdata.dev"
    )


def test_env_adopted_database_id_is_logged(monkeypatch):
    from dbt.adapters.hotdata import credentials as credentials_module

    messages: list[str] = []
    monkeypatch.setattr(credentials_module.logger, "info", lambda msg, *args: messages.append(msg))
    monkeypatch.setenv("HOTDATA_DATABASE", "db_scoped")
    _creds(workspace_id="ws_1")
    assert any("db_scoped" in m and "HOTDATA_DATABASE" in m for m in messages)


def test_validate_passes_in_fully_scoped_environment(monkeypatch):
    monkeypatch.setenv("HOTDATA_API_KEY", "hk_scoped")
    monkeypatch.setenv("HOTDATA_WORKSPACE", "ws_scoped")
    monkeypatch.setenv("HOTDATA_DATABASE", "db_scoped")
    creds = _creds()
    creds.validate_connection_setup()
    assert creds.unique_field == "ws_scoped"


def test_zero_profile_contract_via_from_dict(monkeypatch):
    # The headline contract: `type: hotdata` and nothing else works inside a
    # scoped environment, through dbt's actual construction path (from_dict).
    monkeypatch.setenv("HOTDATA_API_KEY", "hk_scoped")
    monkeypatch.setenv("HOTDATA_WORKSPACE", "ws_scoped")
    monkeypatch.setenv("HOTDATA_DATABASE", "db_scoped")
    creds = HotdataCredentials.from_dict({})
    creds.validate_connection_setup()
    assert creds.resolve_api_key() == "hk_scoped"
    assert creds.workspace_id == "ws_scoped"
    assert creds.database_id == "db_scoped"
    assert creds.database == "default"
    assert creds.schema == "public"
