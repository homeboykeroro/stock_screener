from enum import Enum
from typing import ClassVar

class Count(str, Enum):
    SUM = 'SUM',
    CONSECUTIVE = 'CONSECUTIVE'