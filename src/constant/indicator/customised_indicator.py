from enum import Enum

class CustomisedIndicator(str, Enum):
    CLOSE_CHANGE = 'Close Change',
    HIGH_CHANGE = 'High Change',
    VOLUME_CHANGE = 'Volume Change',
    MA_50_VOLUME = '50MA Volume',
    NORMAL_GAP = 'Normal Gap',
    BODY_GAP = 'Body Gap',
    CANDLE_UPPER_BODY = 'Upper Body',
    CANDLE_LOWER_BODY = 'Lower Body',
    CANDLE_COLOUR = 'Colour',
    CANDLE_BODY_RATIO = 'Body Ratio',
    CANDLE_UPPER_SHADOW_RATIO = 'Upper Shadow Ratio',
    CANDLE_LOWER_SHADOW_RATIO = 'Lower Shadow Ratio'