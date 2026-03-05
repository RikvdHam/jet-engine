from enum import Enum


class Axis(str, Enum):
    ROW = "row"
    COLUMN = "column"
    
    
class SortingDirection(str, Enum):
    ASCENDING = "asc"
    DESCENDING = "desc"    
    

class FieldRole(str, Enum):
    DIMENSION = "dimension"
    MEASURE = "measure"
    FILTER = "filter"
    

class Aggregation(str, Enum):
    SUM = "sum"
    AVG = "avg"
    COUNT = "count"
    COUNT_DISTINCT = "count_distinct"
    MIN = "min"    
    MAX = "max"


class FilterOperator(str, Enum):
    EQ = "="
    NEQ = "!="
    GT = ">"
    GTE = ">="
    LT = "<"
    LTE = "<="
    IN = "in"
    NOT_IN = "not in"
    IS_NULL = "is null"
    IS_NOT_NULL = "is not null"
