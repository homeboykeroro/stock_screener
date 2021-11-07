from enum import Enum

class FilterCritera(str, Enum):
    PATTERN = 'pattern',
    DAY_PERIOD = 'day_period',
    MIN_OBSERVE_DAY = 'min_observe_day',
    CANDLESTICK_PROPERTY_LIST = 'candlestick_property_list',
    UNUSUAL_VOL_MA_COMPARE = 'unusual_vol_ma_compare',
    UNUSUAL_VOL_AND_PRICE_CHANGE_OCCURRENCE = 'unusual_vol_and_price_change_occurrence'
    MIN_UNUSUAL_VOL_EXTENT = 'min_unusual_vol_extent',
    MIN_UNUSUAL_VOL_VAL = 'min_unusual_vol_val'
    

