from fastapi import APIRouter
from datetime import datetime

router = APIRouter()


@router.get("/", summary="Basic health check")
async def health_check():
    return {
        "status": "ok",
        "timestamp": datetime.utcnow()
    }
