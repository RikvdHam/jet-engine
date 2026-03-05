from pathlib import Path
from threading import Thread

from fastapi import FastAPI
from fastapi.responses import JSONResponse
from fastapi.templating import Jinja2Templates
from fastapi.middleware.cors import CORSMiddleware
from slowapi import Limiter
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded

from jet_engine.api.router import api_router
from jet_engine.core.config import settings
from jet_engine.middleware.security_headers import SecurityHeadersMiddleware
from jet_engine.middleware.trusted_host import add_trusted_hosts
from jet_engine.db.base import Base
from jet_engine.db.session import engine

app = FastAPI(
    title="JET Engine",
    docs_url=None if settings.is_production else "/docs",
)

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

limiter = Limiter(key_func=get_remote_address)
app.state.limiter = limiter
app.add_exception_handler(
    RateLimitExceeded,
    lambda request, exc: JSONResponse(
        status_code=429,
        content={"detail": "Rate limit exceeded"}
    )
)


@app.on_event("startup")
def start_stripe_event_worker():
    Base.metadata.create_all(bind=engine)
