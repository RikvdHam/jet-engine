from pydantic import BaseModel


class Pagination(BaseModel):
    limit: int
    offet: int = 0
