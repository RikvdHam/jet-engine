import os
import uuid
import hashlib
from typing import Optional

import polars as pl
from pathlib import Path
from fastapi import APIRouter, UploadFile, File, Form, Depends, HTTPException, Request, Query
from slowapi import Limiter
from slowapi.util import get_remote_address
from sqlalchemy.orm import Session

from jet_engine.infra.core.limiter import limiter
from jet_engine.infra.core.config import settings
from jet_engine.infra.db.session import get_db, get_current_user_id
from jet_engine.infra.db.models import DatasetMapping, SignatureMapping
from jet_engine.app.services.dataset_query_service import get_raw_dataset_page, execute_query
from jet_engine.app.services.dataset_validation_service import validate_dataset
from jet_engine.app.services.dataset_transforming_service import transform_dataset
from jet_engine.app.services.mapping_service import validate_map, save_map
from jet_engine.app.services.dataset_service import get_latest_dataset
from jet_engine.domain.request_models import ViewRequest, MappingRequest
from jet_engine.domain.models import View, Field, Dataset
from jet_engine.infra.core import QueryBuilder


BASE_DIR = Path(__file__).resolve().parent.parent.parent

router = APIRouter()


@router.get("/session/latest")
async def get_session_dataset(
    db: Session = Depends(get_db),
    current_user_id: int = Depends(get_current_user_id)
):
    return await get_latest_dataset(db, current_user_id)


@router.get("/{dataset_id}/data")
async def get(
    dataset_id: str,
    offset: int = Query(0, ge=0),
    limit: int = Query(50, ge=1, le=500),
    db: Session = Depends(get_db)
):
    return get_raw_dataset_page(
        db=db,
        dataset_id=dataset_id,
        offset=offset,
        limit=limit
    )


@router.post("/{dataset_id}/save-mapping")
@limiter.limit("5/minute")
async def save_mapping(
    request: Request,
    dataset_id: str,
    mapping_request: MappingRequest,
    db: Session = Depends(get_db)
):
    await validate_map(mapping_request.mapping)
    await save_map(dataset_id, mapping_request.mapping, db)

    return {"status": "mapped", "canonical_columns": list(mapping_request.mapping.values())}


@router.get("/{dataset_id}/validate")
@limiter.limit("5/minute")
async def validate(
        request: Request,
        dataset_id: str,
        db: Session = Depends(get_db)
):
    return validate_dataset(db, dataset_id)


@router.get("/{dataset_id}/transform")
@limiter.limit("5/minute")
async def transform(
        request: Request,
        dataset_id: str,
        db: Session = Depends(get_db),
        current_user_id: int = Depends(get_current_user_id)
):
    return transform_dataset(db, dataset_id, current_user_id)


@router.post("/{dataset_id}/query")
async def query(
        dataset_id: str,
        request: ViewRequest,
        db: Session = Depends(get_db),
        user_id: int = Depends(get_current_user_id)
):
    return execute_query(db, request, dataset_id, user_id)
