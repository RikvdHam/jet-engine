from enum import Enum


class FieldRole(str, Enum):
    DIMENSION = "dimension"
    MEASURE = "measure"
    FILTER = "filter"
