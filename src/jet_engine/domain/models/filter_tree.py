from typing import Union, List
from uuid import UUID

from pydantic import BaseModel

from jet_engine.domain.enums import LogicalOperator, FilterOperator


class FilterCondition(BaseModel):
    field_id: UUID
    operator: FilterOperator
    value: object
    

class FilterGroup(BaseModel):
    op: LogicalOperator
    conditions: List["FilterNode"]
    

FilterNode = Union[FilterCondition, FilterGroup]
FilterGroup.model_rebuild()
