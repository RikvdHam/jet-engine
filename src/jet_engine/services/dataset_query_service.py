import uuid
from pathlib import Path
from typing import Dict, Optional

import duckdb
from sqlalchemy.orm import Session
from fastapi import HTTPException

from jet_engine.db.models import Dataset, DatasetView
from jet_engine.core.config import settings


BASE_DIR = Path(__file__).resolve().parent.parent


def get_raw_dataset_page(
    db: Session,
    dataset_id: str,
    offset: int,
    limit: int,
):
    dataset = Dataset.load(db, dataset_id)
    if not dataset:
        raise HTTPException(status_code=404, detail="Dataset not found")

    parquet_path = BASE_DIR / settings.storage_raw_dir / dataset.stored_filename
    if not parquet_path.exists():
        raise HTTPException(status_code=404, detail="Parquet file not found")

    query = f"""
            SELECT *
            FROM read_parquet('{parquet_path}')
            LIMIT {limit}
            OFFSET {offset}
        """

    result = duckdb.query(query).to_df()

    return {
        "offset": offset,
        "limit": limit,
        "row_count": dataset.row_count,
        "columns": list(result.columns),
        "data": result.to_dict(orient="records"),
        "has_next": offset + limit < dataset.row_count
    }


def create_view(db: Session, dataset_id: str, current_user_id: int, filters: Dict, dimensions: Dict,
                measures: Dict, parent_view_id: Optional[str] = None) -> DatasetView:
    signature = DatasetView.build_signature(dataset_id, {}, {}, {})
    existing_view = DatasetView.load(db, signature)
    if existing_view:
        return existing_view

    if not parent_view_id:
        view_id = dataset_id
    else:
        view_id = str(uuid.uuid4())

    try:
        view = DatasetView(
            id=view_id,
            dataset_id=dataset_id,
            filters_json=filters,
            dimensions_json=dimensions,
            measures_json=measures,
            signature=signature,
            parent_view_id=parent_view_id,
            created_by=current_user_id
        )
        db.add(view)
        db.commit()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    return view
