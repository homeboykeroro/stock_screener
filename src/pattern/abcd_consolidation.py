from constant.indicator.indicator import Indicator
from constant.computation.logical_comparison import LogicialComparison
from constant.computation.occurrence import Occurrence
from constant.filter_criteria import FilterCriteria

from model.candle_property import CandleProperty
from pattern.pattern_filter import PatternFilter

from utils.filter_util import *

class AbcdConsolidation(PatternFilter):
    def __init__(self, historical_data_df_list: list, filter_criteria_dict: dict) -> None:
        self.__historical_data_df_list = historical_data_df_list
        self.__filter_criteria_dict = filter_criteria_dict

    def filter(self) -> list:
        result_ticker_list = []

        day_period = self.__filter_criteria_dict.get(FilterCriteria.DAY_PERIOD, 20)
        min_observe_day = self.__filter_criteria_dict.get(FilterCriteria.MIN_OBSERVE_DAY, 3)
        
        consolidation_start_idx_offset = self.__filter_criteria_dict.get(FilterCriteria.CONSOLIDATION_START_IDX_OFFSET, 0)
        consolidation_breakout = self.__filter_criteria_dict.get(FilterCriteria.CONSOLIDATION_BREAKOUT, False)
        consolidation_indicator_list = self.__filter_criteria_dict.get(FilterCriteria.CONSOLIDATION_INDICATOR_LIST, [CustomisedIndicator.CANDLE_UPPER_BODY, CustomisedIndicator.CANDLE_LOWER_BODY, Indicator.LOW, Indicator.HIGH])
        consolidation_indicator_list_compare = self.__filter_criteria_dict.get(FilterCriteria.CONSOLIDATION_INDICATOR_LIST_COMPARE, LogicialComparison.OR)
        consolidation_tolerance = self.__filter_criteria_dict.get(FilterCriteria.CONSOLIDATION_TOLERANCE, 6)

        unusual_price_change_property_list = self.__filter_criteria_dict.get(FilterCriteria.UNUSUAL_PRICE_CHANGE_PROPERTY_LIST)
        unusual_candle_property_list = CandleProperty.transform(unusual_price_change_property_list)
        unusual_vol_ma_compare = self.__filter_criteria_dict.get(FilterCriteria.UNUSUAL_VOL_MA_COMPARE, 50)
        min_unusual_vol_extent = self.__filter_criteria_dict.get(FilterCriteria.MIN_UNUSUAL_VOL_EXTENT, 150)
        min_unusual_vol_val = self.__filter_criteria_dict.get(FilterCriteria.MIN_UNUSUAL_VOL_VAL, 300000)
        unusual_vol_and_price_change_occurrence = self.__filter_criteria_dict.get(FilterCriteria.UNUSUAL_VOL_AND_PRICE_CHANGE_OCCURRENCE, Occurrence.LAST)

        for historical_data_df in self.__historical_data_df_list:
            historical_data_df = select_data_by_period(historical_data_df, day_period)
            unusual_vol_and_price_change_idx_df = get_unusual_vol_and_price_change_idx_df(historical_data_df,
                                                                                unusual_candle_property_list, 
                                                                                unusual_vol_ma_compare, min_unusual_vol_extent, min_unusual_vol_val,
                                                                                unusual_vol_and_price_change_occurrence)

            if consolidation_breakout:
                consolidationi_boolean_df = get_consolidation_boolean_df(historical_data_df, 
                                                        unusual_vol_and_price_change_idx_df, consolidation_start_idx_offset,
                                                        consolidation_indicator_list, 
                                                        consolidation_tolerance, consolidation_indicator_list_compare,
                                                        min_observe_day)
                
                close_df = historical_data_df.loc[:, idx[:, Indicator.CLOSE]].rename(columns={Indicator.CLOSE: RuntimeIndicator.COMPARE})
                latest_close_df = close_df.iloc[[-1]].reset_index(drop=True)
                unusual_vol_and_price_change_close_df = get_data_by_idx(close_df, unusual_vol_and_price_change_idx_df)

                breakout_after_consolidation_boolean_df = (latest_close_df >= unusual_vol_and_price_change_close_df)
                result_boolean_df = (consolidationi_boolean_df) & (breakout_after_consolidation_boolean_df)
            else:
                result_boolean_df = get_consolidation_boolean_df(historical_data_df, 
                                                        unusual_vol_and_price_change_idx_df, consolidation_start_idx_offset,
                                                        consolidation_indicator_list, 
                                                        consolidation_tolerance, consolidation_indicator_list_compare,
                                                        min_observe_day)
            
            ticker_to_filtered_result_series = result_boolean_df.any()
            result_ticker_list.extend(ticker_to_filtered_result_series.index[ticker_to_filtered_result_series].get_level_values(0).tolist())

        return result_ticker_list
