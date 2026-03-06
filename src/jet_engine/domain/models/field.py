from uuid import UUID
from typing import List, Optional

from pydantic import BaseModel

from jet_engine.domain.enums import (FieldRole, Aggregation, FilterOperator)


class Field(BaseModel):
    id: UUID
    canonical_name: str
    display_name: str
    description: Optional[str]
    dtype: str
    
    roles: List[FieldRole]
    aggregations: List[Aggregation] = []
    filter_operators: List[FilterOperator] = []
    
    is_required: bool = False
    group: Optional[str] = None
    
    def __str__(self) -> str:
        return (
            f'id: {self.id}, '
            f'canonical_name: {self.canonical_name}, '
            f'dtype: {self.dtype}'
        )
    
    def allows_role(self, role: FieldRole) -> bool:
        return role in self.roles
    
    def allows_aggregation(self, aggregation: Aggregation) -> bool:
        return aggregation in self.aggregations
    
    def allows_filter_operator(self, filter_operator: FilterOperator) -> bool:
        return filter_operator in self.filter_operators
