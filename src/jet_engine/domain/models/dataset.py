from datetime import datetime
from pydantic import BaseModel, ConfigDict


class Dataset(BaseModel):
    id: str
    company_name: str
    fiscal_year: int
    stored_filename: str
    uploaded_by_id: int
    uploaded_at: datetime
    original_filename: str
    signature: str
    row_count: int
    data_hash: str
    last_accessed_at: datetime

    model_config = ConfigDict(from_attributes=True)
