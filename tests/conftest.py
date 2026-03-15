import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from fastapi.testclient import TestClient

from jet_engine.main import app
from jet_engine.infra.db import Base, get_db
from jet_engine.infra.core.config import settings
from jet_engine.infra.core.limiter import limiter


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

    tmp_dir.mkdir()
    raw_dir.mkdir()

    return {
        "base": base_dir,
        "tmp": tmp_dir,
        "raw": raw_dir
    }


@pytest.fixture
def storage_override(tmp_storage, monkeypatch):

    monkeypatch.setattr(settings, "storage_tmp_dir", str(tmp_storage["tmp"]))
    monkeypatch.setattr(settings, "storage_raw_dir", str(tmp_storage["raw"]))

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
