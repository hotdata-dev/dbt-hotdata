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


def test_validate_names_every_missing_field(monkeypatch):
    monkeypatch.delenv("HOTDATA_API_KEY", raising=False)
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
