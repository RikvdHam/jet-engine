from fastapi import APIRouter, Depends, Request
from slowapi import Limiter
from slowapi.util import get_remote_address
from sqlalchemy.orm import Session

from jet_engine.domain.models import Field
from jet_engine.infra.core import field_registry


router = APIRouter()
limiter = Limiter(key_func=get_remote_address)


@router.get("/fields")
@limiter.limit("10/minute")
async def get_fields(request: Request):
    return field_registry.all()
