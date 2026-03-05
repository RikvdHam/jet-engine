import os
import uuid
import hashlib

import polars as pl
from pathlib import Path
from fastapi import APIRouter, UploadFile, File, Form, Depends, HTTPException
from slowapi import Limiter
from slowapi.util import get_remote_address
from sqlalchemy.orm import Session

from jet_engine.core.config import settings
from jet_engine.db.models import Dataset, SignatureMapping
from jet_engine.db.session import get_db


BASE_DIR = Path(__file__).resolve().parent.parent.parent

router = APIRouter()
limiter = Limiter(key_func=get_remote_address)


# --------- Helper functions ---------
def compute_file_hash(file_path: str) -> str:
    sha256 = hashlib.sha256()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(1024*1024), b""):
            sha256.update(chunk)
    return sha256.hexdigest()


@router.post("/csv")
async def upload_csv(
    company_name: str = Form(...),
    fiscal_year: int = Form(...),
    file: UploadFile = File(...),
    # current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    if not file.filename.endswith(".csv"):
        raise HTTPException(status_code=400, detail="Only CSV files allowed")

    dataset_id = str(uuid.uuid4())
    file_name = f"{dataset_id}.csv"
    file_name_parquet = f"{dataset_id}.parquet"
    tmp_file_path = os.path.join(BASE_DIR, settings.storage_tmp_dir, file_name)
    raw_file_path = os.path.join(BASE_DIR, settings.storage_raw_dir, file_name_parquet)

    # ---------- 1. Stream to disk temporarily ----------
    with open(tmp_file_path, "wb") as f:
        while chunk := await file.read(1024 * 1024):
            f.write(chunk)

    # ---------- 2. Convert CSV -> Parquet ----------
    df = pl.read_csv(tmp_file_path)
    df.write_parquet(raw_file_path)

    # ---------- 3. Compute row count & signature ----------
    row_count = df.height
    columns = df.columns
    signature = hashlib.sha256(",".join(sorted(columns)).encode()).hexdigest()
    data_hash = compute_file_hash(raw_file_path)

    # ---------- 4. Save Dataset metadata ----------
    try:
        current_user_id = 1 #TODO: From real user
        dataset = Dataset(
            id=dataset_id,
            company_name=company_name,
            fiscal_year=fiscal_year,
            stored_filename=file_name_parquet,
            # uploaded_by_id=current_user.id,
            uploaded_by_id = current_user_id,
            original_filename=file.filename,
            signature=signature,
            row_count=row_count,
            data_hash=data_hash
        )
        db.add(dataset)
        db.commit()

    except Exception as e:
        if os.path.exists(tmp_file_path):
            os.remove(tmp_file_path)
        if os.path.exists(raw_file_path):
            os.remove(raw_file_path)
        raise HTTPException(status_code=500, detail=str(e))

    # ---------- 5. Remove tmp CSV file ----------
    os.remove(tmp_file_path)

    # ---------- 6. Save Dataset metadata ----------
    suggested_mapping = SignatureMapping.load_mapping(db, signature)

    return {
        "dataset_id": dataset_id,
        "columns": columns,
        "preview": df.head(50).to_dicts(),
        "suggested_mapping": suggested_mapping
    }
