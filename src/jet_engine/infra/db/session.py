from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool


DATABASE_URL = "sqlite:///./dev.db" #TODO: Migrate to PostgreSQL & put into settings (.env)

#TODO: For PostgreSQL don't use StaticPool
engine = create_engine(
    DATABASE_URL,
    connect_args={"check_same_thread": False},  # required for SQLite + threads
    poolclass=StaticPool,                       # DISABLE connection pooling
)

SessionLocal = sessionmaker(
    autocommit=False,
    autoflush=False,
    bind=engine
)


def get_db():
    db: Session = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def get_current_user_id(): #TODO: Extract from JWT/session/header
    return 1
