import pytest
import json
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from fastapi.testclient import TestClient

from jet_engine.main import app
from jet_engine.infra.db import Base, get_db
from jet_engine.infra.core.config import settings
from jet_engine.infra.core.limiter import limiter
from tests.utils import get_test_file


SQLALCHEMY_DATABASE_URL = "sqlite:///./test.db"

engine = create_engine(
    SQLALCHEMY_DATABASE_URL,
    connect_args={"check_same_thread": False}
)

TestingSessionLocal = sessionmaker(bind=engine)


@pytest.fixture(scope="session", autouse=True)
def create_tables():
    Base.metadata.create_all(bind=engine)
    yield
    Base.metadata.drop_all(bind=engine)


@pytest.fixture
def db_session():
    session = TestingSessionLocal()
    try:
        yield session
    finally:
        session.close()


@pytest.fixture(scope="session")
def tmp_storage(tmp_path_factory):
    base_dir = tmp_path_factory.mktemp("storage")

    tmp_dir = base_dir / "tmp"
    raw_dir = base_dir / "raw"
    val_dir = base_dir / "validated"

    tmp_dir.mkdir()
    raw_dir.mkdir()
    val_dir.mkdir()

    return {
        "base": base_dir,
        "tmp": tmp_dir,
        "raw": raw_dir,
        "validated": val_dir
    }


@pytest.fixture
def storage_override(tmp_storage, monkeypatch):

    monkeypatch.setattr(settings, "storage_tmp_dir", str(tmp_storage["tmp"]))
    monkeypatch.setattr(settings, "storage_raw_dir", str(tmp_storage["raw"]))
    monkeypatch.setattr(settings, "storage_validated_dir", str(tmp_storage["validated"]))

    return tmp_storage


@pytest.fixture(autouse=True)
def disable_rate_limiting():
    limiter.enabled = False


@pytest.fixture
def client(db_session, storage_override):

    def override_get_db():
        yield db_session

    app.dependency_overrides[get_db] = override_get_db

    yield TestClient(app)

    app.dependency_overrides.clear()


@pytest.fixture
def uploaded_dataset(client):

    with open(get_test_file("sample_upload.csv"), "rb") as f:
        response = client.post(
            "/api/uploads/csv",
            data={"company_name": "TEST_COMPANY", "fiscal_year": 2025},
            files={"file": ("sample_upload.csv", f, "text/csv")}
        )

    assert response.status_code == 200

    return response.json()


@pytest.fixture
def uploaded_invalid_dataset(client):

    with open(get_test_file("invalid_data.csv"), "rb") as f:
        response = client.post(
            "/api/uploads/csv",
            data={"company_name": "TEST_COMPANY", "fiscal_year": 2025},
            files={"file": ("invalid_data.csv", f, "text/csv")}
        )

    assert response.status_code == 200

    return response.json()


@pytest.fixture
def journal_mapping():

    with open(get_test_file("journal_mapping.json")) as f:
        return json.load(f)
