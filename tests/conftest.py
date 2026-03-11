import pytest
from fastapi.testclient import TestClient

from jet_engine.main import app


@pytest.fixture
def client():
    return TestClient(app)
