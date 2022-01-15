import pandas as pd
import numpy as np

from constant.computation.logical_comparison import LogicialComparison
from constant.indicator.runtime_indicator import RuntimeIndicator

from pandas.core.frame import DataFrame

from utils.numpy_util import normalise_two_dimensional_np_element

idx = pd.IndexSlice

def select_data_by_period(src_df: DataFrame, day_period: int) -> DataFrame:
    end_index = len(src_df)
    start_index = end_index - day_period

    return src_df.iloc[start_index:end_index]

def derive_idx_df(src_df: DataFrame, numeric_idx: bool = True) -> DataFrame:
    if numeric_idx:
        idx_np = src_df.reset_index(drop=True).reset_index().iloc[:, [0]].values
    else:
        idx_np = src_df.reset_index().iloc[:, [0]].values
    
    return pd.DataFrame(np.repeat(idx_np, len(src_df.columns), axis=1), 
                        columns=src_df.columns).rename(columns={src_df.columns.get_level_values(1).values[0]: RuntimeIndicator.INDEX})

def replicate_and_concatenate_df(src_df: DataFrame, repeat_times: int, repeat_axis = 0) -> DataFrame:
    return pd.concat([src_df] * repeat_times, axis=repeat_axis).reset_index(drop=True)

def get_data_by_idx(src_df: DataFrame, src_idx_df: DataFrame) -> DataFrame:
    idx_df = derive_idx_df(src_df)

    if len(src_idx_df) == 1:
        idx_boolean_df = idx_df.eq(src_idx_df.values)
        result_df = src_df.where(idx_boolean_df.values).bfill().iloc[[0]].reset_index(drop=True)
    elif len(src_df) == len(src_idx_df):
        result_df = src_df.where(src_idx_df.notna().values)

    return result_df

def get_data_by_idx_range(src_df: DataFrame, start_idx_df: DataFrame, max_range: int=None) -> DataFrame:
    idx_df = derive_idx_df(src_df)

    if max_range != None:
        end_idx_df = start_idx_df.add(max_range)
        idx_boolean_df = (idx_df.ge(start_idx_df.values)) & (idx_df.le(end_idx_df.values))
    else:
        idx_boolean_df = idx_df.ge(start_idx_df.values)

    return src_df.where(idx_boolean_df.values)

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
    elif boolean_df_list == None:
        raise Exception('None Boolean Comparison List')
    elif len(boolean_df_list) == 0:
        raise Exception('Empty Boolean Comparison List')
    
    return result_boolean_df

def normalise_data_by_idx(src_df: DataFrame, idx_df: DataFrame, repeat_times: int) -> DataFrame:
    data_df = get_data_by_idx(src_df, idx_df)
    normalised_np = normalise_two_dimensional_np_element(data_df.values.T)
    
    normalised_size = normalised_np[0].size
    ticker_list = src_df.columns.get_level_values(0).unique()
    ref_val_col_list = [f'Ref {ref_no + 1}' for ref_no in np.arange(normalised_size)]

    numeric_sequence_index = np.arange(repeat_times)
    ticker_to_ref_val_column = pd.MultiIndex.from_product([ticker_list, ref_val_col_list])

    return pd.DataFrame(np.array([normalised_np.flatten.tolist()] * repeat_times), 
                        columns=ticker_to_ref_val_column,
                        index=numeric_sequence_index)