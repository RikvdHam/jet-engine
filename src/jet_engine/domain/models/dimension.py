from uuid import UUID

from pydantic import BaseModel

from jet_engine.domain.enums import Axis


class Dimension(BaseModel):
    field_id: UUID
    axis: Axis