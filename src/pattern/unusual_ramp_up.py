from constant.indicator.indicator import Indicator
from constant.computation.occurrence import Occurrence
from constant.filter_criteria import FilterCriteria

from model.candle_property import CandleProperty
from pattern.pattern_filter import PatternFilter

from utils.filter_util import *

class UnusualRampUp(PatternFilter):
    def __init__(self, historical_data_df_list: list, filter_criteria_dict: dict) -> None:
        self.__historical_data_df_list = historical_data_df_list
        self.__filter_criteria_dict = filter_criteria_dict

    def filter(self) -> list:
        result_ticker_list = []

        day_period = self.__filter_criteria_dict.get(FilterCriteria.DAY_PERIOD, 12)
        min_observe_day = self.__filter_criteria_dict.get(FilterCriteria.MIN_OBSERVE_DAY, 3)
        
        unusual_price_change_property_list = self.__filter_criteria_dict.get(FilterCriteria.UNUSUAL_PRICE_CHANGE_PROPERTY_LIST)
        unusual_candle_property_list = CandleProperty.transform(unusual_price_change_property_list)
        unusual_vol_ma_compare = self.__filter_criteria_dict.get(FilterCriteria.UNUSUAL_VOL_MA_COMPARE, 50)
        min_unusual_vol_extent = self.__filter_criteria_dict.get(FilterCriteria.MIN_UNUSUAL_VOL_EXTENT, 150)
        min_unusual_vol_val = self.__filter_criteria_dict.get(FilterCriteria.MIN_UNUSUAL_VOL_VAL, 300000)
        unusual_vol_and_price_change_occurrence = self.__filter_criteria_dict.get(FilterCriteria.UNUSUAL_VOL_AND_PRICE_CHANGE_OCCURRENCE, Occurrence.FIRST)

        for historical_data_df in self.__historical_data_df_list:
            historical_data_df = select_data_by_period(historical_data_df, day_period)
            unusual_vol_and_price_change_idx_df = get_unusual_vol_and_price_change_idx_df(historical_data_df,
                                                                                unusual_candle_property_list, 
                                                                                unusual_vol_ma_compare, min_unusual_vol_extent, min_unusual_vol_val,
                                                                                unusual_vol_and_price_change_occurrence)

            min_observe_day_df = unusual_vol_and_price_change_idx_df.apply(lambda x : day_period - x)
            min_observe_day_boolean_df = (min_observe_day_df >= min_observe_day).rename(columns={RuntimeIndicator.INDEX: RuntimeIndicator.COMPARE})
            
            close_df = historical_data_df.loc[:, idx[:, Indicator.CLOSE]]
            low_df = historical_data_df.loc[:, idx[:, Indicator.LOW]]

            gap_fill_value_df = get_data_by_idx(close_df, unusual_vol_and_price_change_idx_df.sub(1)).rename(columns={Indicator.CLOSE: RuntimeIndicator.COMPARE})
            current_low_df = low_df.iloc[[-1]].rename(columns={Indicator.LOW: RuntimeIndicator.COMPARE}).reset_index(drop=True)

            filled_gap_range_boolean_df = (current_low_df >= gap_fill_value_df)
            result_boolean_df = (min_observe_day_boolean_df) & (filled_gap_range_boolean_df)

            ticker_to_filtered_result_series = result_boolean_df.any()
            result_ticker_list.extend(ticker_to_filtered_result_series.index[ticker_to_filtered_result_series].get_level_values(0).tolist())

        return result_ticker_list
