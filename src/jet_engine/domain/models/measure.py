from uuid import UUID
from typing import List

from pydantic import BaseModel

from jet_engine.domain.enums import Aggregation


class Measure(BaseModel):
    field_id: UUID
    aggregations: List[Aggregation]
    
    def expand(self):
        return [
            MeasureSpec(field_id=self.field_id, aggregation=a)
            for a in self.aggregations
        ]    
    
    
class MeasureSpec(BaseModel):
    field_id: UUID
    aggregation: Aggregation
