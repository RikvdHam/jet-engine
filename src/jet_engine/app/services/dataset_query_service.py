import os
import uuid
import duckdb
from pathlib import Path
from typing import Dict, Optional

import duckdb
from sqlalchemy.orm import Session
from fastapi import HTTPException
from fastapi.responses import StreamingResponse

from jet_engine.infra.db.models import Dataset, ViewORM
from jet_engine.infra.core import QueryBuilder
from jet_engine.infra.core.config import settings
from jet_engine.domain.models import View
from jet_engine.domain.request_models import ViewRequest


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


def execute_query(db: Session, request: ViewRequest, dataset_id: str, user_id: int):
    # --- 1. Create semantic model
    view = View.from_request(request, dataset_id, user_id)

    # --- 2. Create query
    compiled_query = QueryBuilder.build(view)

    # --- 3. Execute query
    conn = duckdb.connect()
    result = conn.execute(compiled_query.sql, compiled_query.params)
    arrow_table = result.arrow()

    # --- 4. Save view in DB

    # --- 5. Response serialization
    return _arrow_response(arrow_table)


def _arrow_response(table):

    sink = io.BytesIO()

    with ipc.new_stream(sink, table.schema) as writer:
        writer.write_table(table)

    return Response(
        content=sink.getvalue(),
        media_type="application/vnd.apache.arrow.stream"
    )

#TODO: Create this again

# def create_view(db: Session, dataset_id: str, current_user_id: int, filters: Dict, dimensions: Dict,
#                 measures: Dict, parent_view_id: Optional[str] = None) -> DatasetView:
#     signature = DatasetView.build_signature(dataset_id, {}, {}, {})
#     existing_view = DatasetView.load(db, signature)
#     if existing_view:
#         return existing_view
#
#     if not parent_view_id:
#         view_id = dataset_id
#     else:
#         view_id = str(uuid.uuid4())
#
#     try:
#         view = DatasetView(
#             id=view_id,
#             dataset_id=dataset_id,
#             filters_json=filters,
#             dimensions_json=dimensions,
#             measures_json=measures,
#             signature=signature,
#             parent_view_id=parent_view_id,
#             created_by=current_user_id
#         )
#         db.add(view)
#         db.commit()
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=str(e))
#
#     return view
