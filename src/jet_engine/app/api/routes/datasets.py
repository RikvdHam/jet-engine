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

from jet_engine.infra.core.config import settings
from jet_engine.infra.db.models import Dataset, DatasetMapping, SignatureMapping
from jet_engine.infra.db.session import get_db, get_current_user_id
from jet_engine.app.services.dataset_query_service import get_raw_dataset_page
from jet_engine.app.services.dataset_validation_service import validate_dataset
from jet_engine.app.services.dataset_transforming_service import transform_dataset
from jet_engine.domain.request_models import ViewRequest
from jet_engine.domain.models import View
from jet_engine.domain.models import Field


BASE_DIR = Path(__file__).resolve().parent.parent.parent

router = APIRouter()
limiter = Limiter(key_func=get_remote_address)


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
async def save_mapping(
    dataset_id: str,
    mapping: dict,  # raw_column -> field_id
    db: Session = Depends(get_db)
):
    dataset = Dataset.load(db, dataset_id)
    if not dataset:
        raise HTTPException(status_code=404, detail="Dataset not found")

    # Save mapping for this dataset
    existing_mapping = DatasetMapping.load(db, dataset_id)
    if existing_mapping:
        existing_mapping.mapping_json = mapping
    else:
        db.add(DatasetMapping(dataset_id=dataset_id, mapping_json=mapping))

    # Save mapping for future reference if new signature
    existing_signature = SignatureMapping.load_mapping(db, dataset.signature)
    if not existing_signature:
        db.add(SignatureMapping(signature=dataset.signature, mapping_json=mapping))

    db.commit()

    return {"status": "mapped", "canonical_columns": list(mapping.values())}


@router.get("/{dataset_id}/validate")
async def validate(
        dataset_id: str,
        db: Session = Depends(get_db)
):
    return validate_dataset(db, dataset_id)


@router.get("/{dataset_id}/transform")
async def transform( #TODO: AFTER TRANSFORM: GET LAST VIEW, SO MAKE VIEW OF TRANSFORMED DATASET
        dataset_id: str,
        db: Session = Depends(get_db),
        # current_user: User = Depends(get_current_user), #TODO: Build get_current_user, so we can use this (default 123..)
):
    return transform_dataset(db, dataset_id, {})


@router.post("/{dataset_id}/query")
async def query(
        dataset_id: str,
        request: ViewRequest,
        user_id: int = Depends(get_current_user_id)
):
    view = View.from_request(request, dataset_id, user_id)
    
    return view #TODO: TEST!