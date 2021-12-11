from enum import Enum

class Pattern(str, Enum):
    ABCD_CONSOLIDATION = 'abcd_consolidation',
    UNUSUAL_RAMP_UP = 'unusual_ramp_up'