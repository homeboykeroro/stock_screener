import pandas as pd
import numpy as np

from model.pattern.pattern_filter import PatternFilter

class AscendingTriange(PatternFilter):
    def __init__(self, historical_data_df_list: list, **kwargs) -> None:
        super().__init__(historical_data_df_list, **kwargs)
        self.__historical_data_df_list

    def filter(self, kwargs):
        result_ticker_list = []

        day_period = kwargs.get(FilterCritera.DAY_PERIOD, 30)
        min_observe_day = kwargs.get(FilterCritera.MIN_OBSERVE_DAY, 3)

        unusual_vol_ma_compare = kwargs.get(FilterCritera.UNUSUAL_VOL_MA_COMPARE, 50)
        unusual_vol_and_price_change_occurrence = kwargs.get(FilterCritera.UNUSUAL_VOL_AND_PRICE_CHANGE_OCCURRENCE, 'LAST')
        min_unusual_vol_extent = kwargs.get(FilterCritera.MIN_UNUSUAL_VOL_EXTENT, 150)
        min_unusual_vol_val = kwargs(FilterCritera.MIN_UNUSUAL_VOL_VAL, 300000)

        for index, historical_data_df in enumerate(historical_data_df_list):
            historical_data_df = select_data_by_time_period(historical_data_df, day_period)
            unusual_vol_and_upside_idx_df = get_unusual_vol_and_upside_idx_df(historical_data_df,
                                                                                unusual_upside_indicators, 
                                                                                unusual_vol_ma_compare, min_unusual_vol_extent, min_unusual_vol_val,
                                                                                unusual_vol_and_price_change_occurrence)

            ticker_to_filtered_result_series = result_boolean_df.any()
            result_ticker_list.extend(ticker_to_filtered_result_series.index[ticker_to_filtered_result_series].get_level_values(0).tolist())

        return result_ticker_list
