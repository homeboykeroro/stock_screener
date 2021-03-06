from typing import List
import numpy as np
import pandas as pd

from constant.computation.logical_comparison import LogicialComparison
from constant.computation.occurrence import Occurrence
from constant.indicator.indicator import Indicator
from constant.indicator.customised_indicator import CustomisedIndicator
from constant.indicator.runtime_indicator import RuntimeIndicator

from model.candle_property import CandleProperty

from utils.dataframe_util import *

from pandas.core.frame import DataFrame

idx = pd.IndexSlice

def get_unusual_vol_and_price_change_idx_df(
            historical_data_df: DataFrame,
            unusual_price_change_property_list: List[CandleProperty], 
            unusual_vol_ma_compare: int, min_unusual_vol_extent: float, min_unusual_vol_val: int,
            unusual_vol_and_price_change_occurrence: str) -> DataFrame:
    unusual_vol_boolean_df = get_unusual_vol_boolean_df(historical_data_df, unusual_vol_ma_compare, min_unusual_vol_extent, min_unusual_vol_val).rename(columns={Indicator.VOLUME: RuntimeIndicator.COMPARE})
    price_change_property_boolean_df = get_price_change_property_boolean_df(historical_data_df, unusual_price_change_property_list).rename(columns={RuntimeIndicator.CANDLE: RuntimeIndicator.COMPARE})
    unusual_vol_and_price_change_property_boolean_df = (price_change_property_boolean_df) & (unusual_vol_boolean_df)
    
    idx_df = derive_idx_df(historical_data_df.loc[:, idx[:, Indicator.CLOSE]])

    if unusual_vol_and_price_change_occurrence == Occurrence.FIRST:
        unusual_vol_and_price_change_property_idx_df = idx_df.where(unusual_vol_and_price_change_property_boolean_df.values).fillna(method='bfill').iloc[[0]]
    elif unusual_vol_and_price_change_occurrence == Occurrence.LAST:
        unusual_vol_and_price_change_property_idx_df = idx_df.where(unusual_vol_and_price_change_property_boolean_df.values).fillna(method='ffill').iloc[[-1]]
    elif unusual_vol_and_price_change_occurrence == Occurrence.ALL:
        unusual_vol_and_price_change_property_idx_df = idx_df.where(unusual_vol_and_price_change_property_boolean_df.values)
    else:
        raise Exception(f'Unusual Volume and Price Occurrence of {unusual_vol_and_price_change_occurrence} Not Found')
    
    return unusual_vol_and_price_change_property_idx_df.rename(index={unusual_vol_and_price_change_property_idx_df.index.values[0]: 0})

def get_unusual_vol_boolean_df(historical_data_df: DataFrame, vol_ma: int, vol_extent: float, vol_val: int) -> DataFrame:
    vol_df = historical_data_df.loc[:, idx[:, Indicator.VOLUME]]
    vol_ma_df = historical_data_df.loc[:, idx[:, f'{vol_ma}MA Volume']]

    vol_vol_ma_diff_df = ((vol_df.sub(vol_ma_df.values)).div(vol_ma_df.values)).mul(100)
    unusual_vol_boolean_df = (vol_vol_ma_diff_df >= vol_extent)

    if vol_val != None:
        vol_val_boolean_df = (vol_df >= vol_val)
        result_boolean_df = (unusual_vol_boolean_df) & (vol_val_boolean_df)
        return result_boolean_df
    else:
        return unusual_vol_boolean_df

def get_price_change_property_boolean_df(
            historical_data_df: DataFrame,
            unusual_price_change_property_list: List[CandleProperty]) -> DataFrame:
    candle_property_boolean_df_list = []

    for candle_property in unusual_price_change_property_list:
        candle_property_boolean_df = get_candlestick_type_boolean_df(historical_data_df, candle_property)
        candle_property_boolean_df_list.append(candle_property_boolean_df)

    result_boolean_df = logical_compare_boolean_df(candle_property_boolean_df_list, LogicialComparison.OR)
    return result_boolean_df

def get_daily_volume_val_boolean_df(historical_data_df: DataFrame, volume: int) -> DataFrame:
    vol_df = historical_data_df.loc[:, idx[:, Indicator.VOLUME]]
    vol_boolean_df = (vol_df >= volume).all()

    result_boolean_df = pd.DataFrame(vol_boolean_df.all()).T
    return result_boolean_df

def get_candlestick_type_boolean_df(historical_data_df: DataFrame, candle_property: CandleProperty) -> DataFrame:
    candle_colour = candle_property.colour
    close_pct = candle_property.close_pct
    high_pct = candle_property.high_pct
    gap_pct = candle_property.gap_pct
    body_gap_pct = candle_property.body_gap_pct
    upper_shadow_ratio = candle_property.upper_shadow_ratio
    lower_shadow_ratio = candle_property.lower_shadow_ratio
    body_ratio = candle_property.body_ratio

    close_pct_df = historical_data_df.loc[:, idx[:, CustomisedIndicator.CLOSE_CHANGE]].rename(columns={CustomisedIndicator.CLOSE_CHANGE: RuntimeIndicator.COMPARE})
    high_pct_df = historical_data_df.loc[:, idx[:, CustomisedIndicator.HIGH_CHANGE]].rename(columns={CustomisedIndicator.HIGH_CHANGE: RuntimeIndicator.COMPARE})
    normal_gap_pct_df = historical_data_df.loc[:, idx[:, CustomisedIndicator.NORMAL_GAP]].rename(columns={CustomisedIndicator.NORMAL_GAP: RuntimeIndicator.COMPARE})
    body_gap_pct_df = historical_data_df.loc[:, idx[:, CustomisedIndicator.BODY_GAP]].rename(columns={CustomisedIndicator.BODY_GAP: RuntimeIndicator.COMPARE})
    candle_colour_df = historical_data_df.loc[:, idx[:, CustomisedIndicator.CANDLE_COLOUR]].rename(columns={CustomisedIndicator.CANDLE_COLOUR: RuntimeIndicator.COMPARE})

    property_boolean_df_list = []

    if candle_colour != None:
        candle_colour_boolean_df = (candle_colour_df == candle_colour)
        property_boolean_df_list.append(candle_colour_boolean_df)
    
    if close_pct != None:
        if close_pct >= 0:
            close_pct_boolean_df = (close_pct_df >= close_pct)
        else:
            close_pct_boolean_df = (close_pct_df <= close_pct)
        property_boolean_df_list.append(close_pct_boolean_df)
    
    if high_pct != None:
        if high_pct >= 0:
            high_pct_boolean_df = (high_pct_df >= high_pct)
        property_boolean_df_list.append(high_pct_boolean_df)
    
    if gap_pct != None:
        if gap_pct >= 0:
            gap_pct_boolean_df = (normal_gap_pct_df >= gap_pct)
        else:
            gap_pct_boolean_df = (normal_gap_pct_df <= gap_pct)
        property_boolean_df_list.append(gap_pct_boolean_df)
    
    if body_gap_pct != None:
        if body_gap_pct >= 0:
            body_gap_pct_boolean_df = (body_gap_pct_df >= body_gap_pct)
        else:
            body_gap_pct_boolean_df = (body_gap_pct_df <= body_gap_pct)
        property_boolean_df_list.append(body_gap_pct_boolean_df)
    
    if upper_shadow_ratio != None:
        upper_shadow_ratio_df = historical_data_df.loc[:, idx[:, CustomisedIndicator.CANDLE_UPPER_SHADOW_RATIO]].rename(columns={CustomisedIndicator.CANDLE_UPPER_SHADOW_RATIO: RuntimeIndicator.COMPARE})
        upper_shadow_boolean_df = (upper_shadow_ratio_df >= upper_shadow_ratio)
        property_boolean_df_list.append(upper_shadow_boolean_df)
    
    if lower_shadow_ratio != None:
        lower_shadow_ratio_df = historical_data_df.loc[:, idx[:, CustomisedIndicator.CANDLE_LOWER_SHADOW_RATIO]].rename(columns={CustomisedIndicator.CANDLE_LOWER_SHADOW_RATIO: RuntimeIndicator.COMPARE})
        lower_shadow_boolean_df = (lower_shadow_ratio_df >= lower_shadow_ratio)
        property_boolean_df_list.append(lower_shadow_boolean_df)
    
    if body_ratio != None:
        body_ratio_df = historical_data_df.loc[:, idx[:, CustomisedIndicator.CANDLE_BODY_RATIO]].rename(columns={CustomisedIndicator.CANDLE_BODY_RATIO: RuntimeIndicator.COMPARE})
        body_shadow_boolean_df = (body_ratio_df >= body_ratio)
        property_boolean_df_list.append(body_shadow_boolean_df)

    result_boolean_df = logical_compare_boolean_df(property_boolean_df_list, LogicialComparison.AND)
    return result_boolean_df.rename(columns={RuntimeIndicator.COMPARE: RuntimeIndicator.CANDLE})

def get_consolidation_boolean_df( 
            historical_data_df: DataFrame,
            start_idx_df: DataFrame, start_idx_offset: int,
            indicator_list: list,
            tolerance: float, compare: LogicialComparison,
            min_observe_day: int) -> DataFrame:

    full_day_range = len(historical_data_df)

    min_tolerance = 1 - (tolerance/ 100)
    max_tolerance = 1 + (tolerance/ 100)
    consolidation_boolean_df_list = []

    for indicator in indicator_list:
        indicator_data_df = get_data_by_idx_range(historical_data_df.loc[:, idx[:, indicator]], start_idx_df.add(start_idx_offset))
        
        repeat_data_df = pd.DataFrame(np.repeat(indicator_data_df.values, full_day_range, axis=0), 
                                        columns=indicator_data_df.columns, 
                                        index=np.repeat(indicator_data_df.reset_index().index.tolist(), full_day_range, axis=0)
                                    ).rename(columns={indicator: RuntimeIndicator.COMPARE})
        expand_data_df = replicate_and_concatenate_df(indicator_data_df, full_day_range).rename(columns={indicator: RuntimeIndicator.COMPARE})
        
        min_range_df = expand_data_df.mul(min_tolerance).set_index(repeat_data_df.index)
        max_range_df = expand_data_df.mul(max_tolerance).set_index(repeat_data_df.index)
        in_range_boolean_df = (repeat_data_df >= min_range_df) & (repeat_data_df <= max_range_df)

        observe_day_df = start_idx_df.apply(lambda x: full_day_range - x - start_idx_offset)
        min_observe_day_boolean_df = (observe_day_df >= min_observe_day)
        min_observe_day_df = (observe_day_df.where(min_observe_day_boolean_df.values)).reset_index(drop=True).rename(columns={RuntimeIndicator.INDEX: RuntimeIndicator.COMPARE})

        sum_boolean_count_df = in_range_boolean_df.groupby(in_range_boolean_df.index).sum()
        last_observe_day_sum_boolean_count_df = sum_boolean_count_df.iloc[[-1]].reset_index(drop=True)
        full_range_consolidation_boolean_df = (last_observe_day_sum_boolean_count_df == min_observe_day_df)
        consolidation_boolean_df = pd.DataFrame(full_range_consolidation_boolean_df.any()).T 

        consolidation_boolean_df_list.append(consolidation_boolean_df)
        
    result_boolean_df = logical_compare_boolean_df(consolidation_boolean_df_list, compare)
    return result_boolean_df