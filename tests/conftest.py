"""Shared fixtures and configuration for the test suite."""
from __future__ import annotations

import os

import pytest


@pytest.fixture(autouse=True)
def set_gcp_project(monkeypatch):
    """Ensure GCP_PROJECT_ID is always set so Settings.load() doesn't raise."""
    monkeypatch.setenv("GCP_PROJECT_ID", "test-project")
