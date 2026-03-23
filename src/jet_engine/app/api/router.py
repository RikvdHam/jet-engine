from fastapi import APIRouter

from jet_engine.app.api.routes import (uploads, datasets, meta, health, app)

api_router = APIRouter()

api_router.include_router(uploads.router, prefix="/api/uploads", tags=["uploads"])
api_router.include_router(datasets.router, prefix="/api/datasets", tags=["datasets"])
api_router.include_router(meta.router, prefix="/api/meta", tags=["meta"])
api_router.include_router(health.router, prefix="/api/health", tags=["health"])
api_router.include_router(app.router, prefix="/app", tags=["app"])
