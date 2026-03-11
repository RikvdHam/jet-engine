from pydantic import BaseModel
from typing import List


class Query(BaseModel):
    sql: str
    params: List
