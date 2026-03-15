from fastapi import APIRouter, UploadFile, File, Form, Depends, HTTPException, Request
from slowapi import Limiter
from slowapi.util import get_remote_address
from sqlalchemy.orm import Session

from jet_engine.infra.db.session import get_db, get_current_user_id
from jet_engine.app.services.dataset_service import process_csv_upload


router = APIRouter()
limiter = Limiter(key_func=get_remote_address)


@router.post("/csv")
@limiter.limit("5/minute")
async def upload_csv(
    request: Request,
    company_name: str = Form(...),
    fiscal_year: int = Form(...),
    file: UploadFile = File(...),
    current_user_id: int = Depends(get_current_user_id),
    db: Session = Depends(get_db)
):
    if not file.filename.endswith(".csv"):
        raise HTTPException(status_code=400, detail="Only CSV files allowed")

    return await process_csv_upload(company_name, fiscal_year, file, current_user_id, db)
