from pathlib import Path
from threading import Thread

from fastapi import FastAPI
from fastapi.responses import JSONResponse
from fastapi.templating import Jinja2Templates
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from slowapi.middleware import SlowAPIMiddleware
from slowapi import Limiter
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded
from contextlib import asynccontextmanager

from jet_engine.app.api.router import api_router
from jet_engine.infra.core.config import settings
from jet_engine.infra.middleware.security_headers import SecurityHeadersMiddleware
from jet_engine.infra.middleware.trusted_host import add_trusted_hosts
from jet_engine.infra.db.base import Base
from jet_engine.infra.db.session import engine
from jet_engine.infra.core.limiter import limiter


# Determine absolute path to static folder
BASE_DIR = Path(__file__).resolve().parent
STATIC_DIR = BASE_DIR / "app" / "static"


@asynccontextmanager
async def lifespan(application: FastAPI):

    Base.metadata.create_all(bind=engine)

    application.state.cache = {}
    print("Cache initialized")

    yield

    print("App shutting down")


app = FastAPI(
    title="JET Engine",
    docs_url=None if settings.is_production else "/docs",
    lifespan=lifespan
)

app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")

app.include_router(api_router)

# Security headers
app.add_middleware(SecurityHeadersMiddleware)

# Trusted hosts (prod only)
if settings.is_production:
    add_trusted_hosts(app, settings.allowed_hosts)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["https://jet-engine.com"], #TODO: Aanpassen!
    allow_credentials=True,
    allow_methods=["GET", "POST"],
    allow_headers=["Authorization", "Content-Type"],
)

app.state.limiter = limiter
app.add_exception_handler(
    RateLimitExceeded,
    lambda request, exc: JSONResponse(
        status_code=429,
        content={"detail": "Rate limit exceeded"}
    )
)
app.add_middleware(SlowAPIMiddleware)
