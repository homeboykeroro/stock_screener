import pandas as pd
import numpy as np

from constant.computation.logical_comparison import LogicialComparison
from constant.indicator.runtime_indicator import RuntimeIndicator

from pandas.core.frame import DataFrame

idx = pd.IndexSlice

def select_data_by_period(src_df: DataFrame, day_period: int) -> DataFrame:
    end_index = len(src_df)
    start_index = end_index - day_period

    return src_df.iloc[start_index:end_index]

def derive_idx_df(src_df: DataFrame) -> DataFrame:
    idx_np = src_df.reset_index().loc[:, idx['index', :]].values
    return pd.DataFrame(np.repeat(idx_np, len(src_df.columns), axis=1), columns=src_df.columns).rename(columns={src_df.columns.get_level_values(1).values[0]: RuntimeIndicator.INDEX})

def replicate_and_concatenate_df(src_df: DataFrame, repeat_times: int, axis: int) -> DataFrame:
    return pd.concat([src_df] * repeat_times, axis)

def get_data_by_idx(src_df: DataFrame, idx_df: DataFrame) -> DataFrame:
    idx_df = derive_idx_df(src_df)

    if len(idx_df) == 1:
        expand_idx_df = pd.concat([idx_df] * len(src_df)).set_index(idx_df.index)
        idx_boolean_df = (expand_idx_df == idx_df)
    
        result_df = src_df.where(idx_boolean_df.values).fillna(method='bfill').iloc[[0]].reset_index(drop=True)
        result_df = result_df.rename(index={result_df.index.values[0]: 0})
    elif len(src_df) == len(idx_df):
        result_df = src_df.reset_index(drop=True).where(idx_df.notna().values)

    return result_df

def get_data_by_idx_range(src_df: DataFrame, start_idx_df: DataFrame, max_range: int=None) -> DataFrame:
    idx_df = derive_idx_df(src_df)
    expand_start_idx_df = pd.concat([start_idx_df] * len(src_df)).set_index(idx_df.index)

    if max_range != None:
        end_idx_df = start_idx_df.add(max_range)
        expand_end_idx_df = pd.concat([end_idx_df] * len(src_df)).set_index(idx_df.index)
        idx_boolean_df = (idx_df >= expand_start_idx_df) & (idx_df <= expand_end_idx_df)
    else:
        idx_boolean_df = (idx_df >= expand_start_idx_df)

    return src_df.where(idx_boolean_df.values)

def get_consecutive_boolean_count_df(src_df: DataFrame) -> DataFrame:
    return (src_df.cumsum() - src_df.cumsum().where(~src_df.values).ffill().fillna(0))

def logical_compare_boolean_df(boolean_df_list: list, compare: LogicialComparison) -> DataFrame:
    if len(boolean_df_list) > 1:
        for df_idx, boolean_df in enumerate(boolean_df_list):
            if df_idx == 0:
                result_boolean_df = boolean_df
            else:
                if compare == LogicialComparison.OR:
                    result_boolean_df = (result_boolean_df) | (boolean_df)
                elif compare == LogicialComparison.AND:
                    result_boolean_df = (result_boolean_df) & (boolean_df)
                else:
                    raise Exception(f'Logicial Comparison of {compare} Not Found')
    elif len(boolean_df_list) == 1:
        result_boolean_df = boolean_df_list[0]
    elif not boolean_df_list:
        raise Exception('None Boolean Comparison List')
    elif len(boolean_df_list) == 0:
        raise Exception('Empty Boolean Comparison List')
    
    return result_boolean_df