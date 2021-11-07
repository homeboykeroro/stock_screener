from enum import Enum

class CustomisedIndicator(str, Enum):
    CLOSE_CHANGE = 'Close Change',
    HIGH_CHANGE = 'High Change',
    VOLUME_CHANGE = 'Volume Change',
    CANDLE_UPPER_BODY = 'Candle Upper Body',
    CANDLE_LOWER_BODY = 'Candle Lower Body',
    MA_50_VOLUME = '50MA Volume',
    NORMAL_GAP = 'Normal Gap',
    CANDLE_BODY_GAP = 'Candle Body Gap'