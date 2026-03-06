from uuid import UUID

from pydantic import BaseModel

from jet_engine.domain.enums import SortDirection


class Sorting(BaseModel):
    field_id: UUID
    sorting_direction: SortDirection
