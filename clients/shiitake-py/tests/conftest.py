"""Shared fixtures for the shiitake-py end-to-end tests.

The e2e tests need a live server; the k3d suite's ``run.sh`` sets
``SHIITAKE_E2E_URL`` after standing the cluster up. A test that takes the
``shiitake_client`` fixture is skipped when it is unset — the unit tests, which don't take
it, still run.
"""

from __future__ import annotations

import os
from collections.abc import AsyncIterator

import pytest
from shiitake.client import AsyncShiitakeClient

BASE = os.environ.get("SHIITAKE_E2E_URL")
TOKEN = os.environ.get("SHIITAKE_E2E_TOKEN") or None


@pytest.fixture
async def shiitake_client() -> AsyncIterator[AsyncShiitakeClient]:
    """A connected client against the e2e server; skips when none is configured."""
    if not BASE:
        pytest.skip("SHIITAKE_E2E_URL not set")
    async with AsyncShiitakeClient(BASE, auth_token=TOKEN) as c:
        yield c
