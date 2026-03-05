from uuid import UUID
from typing import List, Optional

from jet_engine.models.enums import FieldRole, Aggregation, FilterOperator


class Field:
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
    
    def __init__(self, id, canonical_name, display_name, description, dtype, 
                 roles, aggregations, filter_operators, is_required, 
                 group) -> None:
        self.id = id    
        self.canonical_name = canonical_name
        self.display_name = display_name
        self.description = description
        self.dtype = dtype
        self.roles = roles
        self.aggregations = aggregations
        self.filter_operators = filter_operators
        self.is_required = is_required
        self.group = group

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
