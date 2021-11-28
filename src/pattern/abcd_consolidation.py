from constant.candle.candle_colour import CandleColour

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

        day_period = self.__filter_criteria_dict.get(FilterCriteria.DAY_PERIOD, 30)
        min_observe_day = self.__filter_criteria_dict.get(FilterCriteria.MIN_OBSERVE_DAY, 3)
        
        consolidation_indicator_list = self.__filter_criteria_dict.get(FilterCriteria.CONSOLIDATION_INDICATOR_LIST, [CustomisedIndicator.CANDLE_UPPER_BODY, CustomisedIndicator.CANDLE_LOWER_BODY])
        consolidation_tolerance = self.__filter_criteria_dict.get(FilterCriteria.CONSOLIDATION_TOLERANCE, 5)
        consolidation_count = self.__filter_criteria_dict.get(FilterCriteria.CONSOLIDATION_COUNT, Count.CONSECUTIVE)
        consolidation_compare = self.__filter_criteria_dict.get(FilterCriteria.CONSOLIDATION_COMPARE, LogicialComparison.OR)

        unusual_price_change_property_list = self.__filter_criteria_dict.get(FilterCriteria.UNUSUAL_PRICE_CHANGE_PROPERTY_LIST, [
            CandleProperty(colour=CandleColour.GREEN, close_pct=8, high_pct=0, body_ratio=70)
        ])
        unusual_vol_ma_compare = self.__filter_criteria_dict.get(FilterCriteria.UNUSUAL_VOL_MA_COMPARE, 50)
        min_unusual_vol_extent = self.__filter_criteria_dict.get(FilterCriteria.MIN_UNUSUAL_VOL_EXTENT, 150)
        min_unusual_vol_val = self.__filter_criteria_dict.get(FilterCriteria.MIN_UNUSUAL_VOL_VAL, 300000)
        unusual_vol_and_price_change_occurrence = self.__filter_criteria_dict.get(FilterCriteria.UNUSUAL_VOL_AND_PRICE_CHANGE_OCCURRENCE, Occurrence.FIRST)

        for historical_data_df in self.__historical_data_df_list:
            historical_data_df = select_data_by_period(historical_data_df, day_period)
            unusual_vol_and_price_change_idx_df = get_unusual_vol_and_price_change_idx_df(historical_data_df,
                                                                                unusual_price_change_property_list, 
                                                                                unusual_vol_ma_compare, min_unusual_vol_extent, min_unusual_vol_val,
                                                                                unusual_vol_and_price_change_occurrence)

            consolidationi_boolean_df = get_consolidation_boolean_df(historical_data_df, 
                                                        unusual_vol_and_price_change_idx_df,
                                                        consolidation_indicator_list, 
                                                        consolidation_tolerance, consolidation_count, consolidation_compare,
                                                        min_observe_day)
            
            low_df = historical_data_df.loc[:, idx[:, Indicator.LOW]]
            support_df = get_data_by_idx(low_df, unusual_vol_and_price_change_idx_df)
            low_above_support_df = (low_df >= support_df)
            support_boolean_df = pd.DataFrame(low_above_support_df.all()).T

            result_boolean_df = (consolidationi_boolean_df) & (support_boolean_df)
            ticker_to_filtered_result_series = result_boolean_df.any()
            result_ticker_list.extend(ticker_to_filtered_result_series.index[ticker_to_filtered_result_series].get_level_values(0).tolist())

        return result_ticker_list
