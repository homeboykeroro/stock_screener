from constant.candle.candle_colour import CandleColour

class CandleProperty:
    CLOSE_PCT = 'close_pct'
    HIGH_PCT = 'high_pct'
    NORMAL_GAP_PCT = 'normal_gap_pct'
    BODY_GAP_PCT = 'body_gap_pct'
    COLOUR = 'colour'
    BODY_RATIO = 'body_ratio'
    UPPER_SHADOW_RATIO = 'upper_shadow_ratio'
    LOWER_SHADOW_RATIO = 'lower_shadow_ratio'

    @staticmethod
    def transform(property_dict_list: list) -> list:
        candle_property_list = []

        for property in property_dict_list:
            colour = property.get(CandleProperty.COLOUR)
            close_pct = property.get(CandleProperty.CLOSE_PCT)
            high_pct = property.get(CandleProperty.HIGH_PCT)
            normal_gap_pct = property.get(CandleProperty.NORMAL_GAP_PCT)
            body_gap_pct = property.get(CandleProperty.BODY_GAP_PCT)
            upper_shadow_ratio = property.get(CandleProperty.UPPER_SHADOW_RATIO)
            lower_shadow_ratio = property.get(CandleProperty.LOWER_SHADOW_RATIO)
            body_ratio = property.get(CandleProperty.BODY_RATIO)

            candle_property = CandleProperty(colour, 
                                            close_pct, high_pct,
                                            normal_gap_pct, body_gap_pct, 
                                            upper_shadow_ratio, lower_shadow_ratio, body_ratio)
            
            candle_property_list.append(candle_property)
        
        return candle_property_list

    def __init__(self, colour: CandleColour=None, 
                close_pct: float=None, high_pct: float=None,
                normal_gap_pct: float=None, body_gap_pct: float=None, 
                upper_shadow_ratio: float=None, lower_shadow_ratio: float=None, body_ratio: float=None) -> None:
        
        if upper_shadow_ratio is not None and upper_shadow_ratio < 0:
                raise Exception("Upper Shadow Ratio Can't Be Smaller Than Zero")
        
        if lower_shadow_ratio is not None and lower_shadow_ratio < 0:
            raise Exception("Lower Shadow Ratio Can't Be Smaller Than Zero")

        if body_ratio is not None and body_ratio < 0:
                raise Exception("Body Ratio Can't Be Smaller Than Zero")
        
        if colour is not None and colour != CandleColour.GREEN and colour != CandleColour.RED:
            raise Exception(f'Candle Colour of {colour} Not Found')
        
        if not colour and not close_pct and not high_pct and not normal_gap_pct and not body_gap_pct and not upper_shadow_ratio and not lower_shadow_ratio and not body_ratio:
            raise Exception("Candle Property Can't Be Created with Zero Argument")
        
        self.__colour = colour
        self.__close_pct = close_pct
        self.__high_pct = high_pct
        self.__normal_gap_pct = normal_gap_pct
        self.__body_gap_pct = body_gap_pct
        self.__upper_shadow_ratio = upper_shadow_ratio
        self.__lower_shadow_ratio = lower_shadow_ratio
        self.__body_ratio = body_ratio

    @property
    def colour(self):
        return self.__colour

    @colour.setter
    def colour(self, colour):
        self.__colour = colour
    
    @property
    def close_pct(self):
        return self.__close_pct
    
    @close_pct.setter
    def close_pct(self, close_pct):
        self.__close_pct = close_pct

    @property
    def high_pct(self):
        return self.__high_pct
    
    @high_pct.setter
    def high_pct(self, high_pct):
        self.__high_pct = high_pct

    @property
    def normal_gap_pct(self):
        return self.__normal_gap_pct
    
    @normal_gap_pct.setter
    def gap_pct(self, normal_gap_pct):
        self.__normal_gap_pct = normal_gap_pct

    @property
    def body_gap_pct(self):
        return self.__body_gap_pct
    
    @body_gap_pct.setter
    def body_gap_pct(self, body_gap_pct):
        self.__body_gap_pct = body_gap_pct
    
    @property
    def upper_shadow_ratio(self):
        return self.__upper_shadow_ratio
    
    @upper_shadow_ratio.setter
    def upper_shadow_ratio(self, upper_shadow_ratio):
        self.__upper_shadow_ratio = upper_shadow_ratio
    
    @property
    def lower_shadow_ratio(self):
        return self.__lower_shadow_ratio
    
    @lower_shadow_ratio.setter
    def lower_shadow_ratio(self, lower_shadow_ratio):
        self.__lower_shadow_ratio = lower_shadow_ratio

    @property
    def body_ratio(self):
        return self.__body_ratio
    
    @body_ratio.setter
    def body_ratio(self, body_ratio):
        self.__body_ratio = body_ratio