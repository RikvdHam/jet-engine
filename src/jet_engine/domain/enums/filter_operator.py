from enum import Enum


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
