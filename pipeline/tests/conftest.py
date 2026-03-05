"""
Shared fixtures for pipeline tests.
"""

import os

import pytest


@pytest.fixture(autouse=True)
def _set_env_defaults(monkeypatch):
    """Seed minimal env vars so config.py loads without KeyError.

    After setting env vars we also rebuild the module-level
    ``config`` singleton so every test sees a fresh
    ``PipelineConfig`` instance that reflects the monkeypatched
    environment.
    """
    defaults = {
        "IH_TENANT_ID": "00000000-0000-0000-0000-000000000000",
        "IH_CLIENT_ID": "00000000-0000-0000-0000-000000000001",
        "IH_CLIENT_SECRET": "test-secret",
        "IH_ADLS_ACCOUNT_NAME": "testaccount",
        "IH_ADLS_CONTAINER": "insight-harbor",
        "IH_KEY_VAULT_NAME": "test-kv",
        "IH_LOOKBACK_DAYS": "2",
        "IH_PARTITION_HOURS": "6",
        "IH_SUBDIVISION_THRESHOLD": "950000",
        "IH_BATCH_SIZE": "4",
        "IH_SCHEDULE_CRON": "0 0 2 * * *",
        "IH_DURABLE_TASK_HUB": "ihpipelinehub",
        "IH_ENABLE_M365_USAGE": "true",
        "IH_ENABLE_DSPM": "false",
        "IH_ENABLE_AUTO_COMPLETENESS": "false",
        "IH_ACTIVITY_TYPES": "",
        "IH_USER_IDS": "",
        "IH_SERVICE_TYPES": "",
        "IH_RECORD_TYPES": "",
        "IH_TEAMS_WEBHOOK_URL": "",
    }
    for k, v in defaults.items():
        monkeypatch.setenv(k, v)

    # Re-initialise the *existing* config singleton in-place so that
    # every module that did ``from shared.config import config`` still
    # references the same object but with refreshed values.
    import shared.config as cfg_mod
    cfg_mod.config.__init__()
