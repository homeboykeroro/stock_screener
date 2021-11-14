from enum import Enum

class FilterCriteria(str, Enum):
    PATTERN = 'pattern',
    DAY_PERIOD = 'day_period',
    MIN_OBSERVE_DAY = 'min_observe_day',
    UNUSUAL_PRICE_CHANGE_PROPERTY_LIST = 'unusual_price_change_property_list',
    UNUSUAL_VOL_MA_COMPARE = 'unusual_vol_ma_compare',
    MIN_UNUSUAL_VOL_EXTENT = 'min_unusual_vol_extent',
    MIN_UNUSUAL_VOL_VAL = 'min_unusual_vol_val'
    UNUSUAL_VOL_AND_PRICE_CHANGE_OCCURRENCE = 'unusual_vol_and_price_change_occurrence',
    CONSOLIDATION_INDICATOR_LIST = 'consolidation_indicator_list',
    CONSOLIDATION_TOLERANCE = 'consolidation_tolerance',
    CONSOLIDATION_COUNT = 'consolidation_count',
    CONSOLIDATION_COMPARE = 'consolidation_compare'

