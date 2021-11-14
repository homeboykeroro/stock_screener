from enum import Enum

class Occurrence(str, Enum):
    FIRST = 'FIRST',
    LAST = 'LAST',
    ALL = 'ALL'