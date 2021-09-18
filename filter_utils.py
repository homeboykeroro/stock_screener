import numpy as np
from numpy import typing
import pandas as pd
from pandas.core.frame import DataFrame
from os import close
from typing import Union

idx = pd.IndexSlice

def select_data_by_time_period( src_df: DataFrame, day_period: int ) -> DataFrame:
    end_index = len( src_df )
    start_index = end_index - day_period

    return src_df.iloc[ start_index:end_index ]

def get_unusual_vol_and_upside_idx_df( 
            historical_data_df: DataFrame,
            unusual_gap_up_pct: float, unusual_close_pct: float, 
            compare_unusual_vol_ma: int, unusual_vol_extent: float, unusual_vol_val: int,
            unusual_vol_and_upside_occurrence: str ) -> DataFrame:
    unusual_vol_boolean_df = get_unusual_vol_boolean_df( historical_data_df, compare_unusual_vol_ma, unusual_vol_extent, unusual_vol_val ).rename( columns={ 'Unusual Vol': 'Compare' } )
    upside_change_boolean_df = get_upside_change_boolean_df( historical_data_df, unusual_gap_up_pct, unusual_close_pct ).rename( columns={ 'Up': 'Compare' } )
    unusual_vol_and_upside_change_boolean_df = ( upside_change_boolean_df ) & ( unusual_vol_boolean_df )
    
    idx_df = derive_idx_df_from_src_df( historical_data_df.loc[ :, idx[ :, 'Close' ] ] )

    if unusual_vol_and_upside_occurrence == 'FIRST':
        unusual_vol_and_upside_idx_df = idx_df.where( unusual_vol_and_upside_change_boolean_df.values ).fillna( method='bfill' ).iloc[ [ 0 ] ]
    if unusual_vol_and_upside_occurrence == 'LAST':
        unusual_vol_and_upside_idx_df = idx_df.where( unusual_vol_and_upside_change_boolean_df.values ).fillna( method='ffill' ).iloc[ [ -1 ] ]

    return unusual_vol_and_upside_idx_df.rename( index={ unusual_vol_and_upside_idx_df.index.values[ 0 ]: 0 } )

def get_unusual_vol_boolean_df( src_df: DataFrame, vol_ma: int, vol_extent: float, vol_val: int ) -> DataFrame:
    vol_df = src_df.loc[ :, idx[ :, 'Volume' ] ]
    vol_ma_df = src_df.loc[ :, idx[ :, ( str( vol_ma ) + 'MA Vol' ) ] ]

    vol_vol_ma_diff_df = ( ( vol_df.sub( vol_ma_df.values ) ).div( vol_ma_df.values ) ).mul( 100 )
    unusual_vol_boolean_df = ( vol_vol_ma_diff_df >= vol_extent )

    if vol_val != None:
        vol_val_boolean_df = ( vol_df >= vol_val )
        result_boolean_df = ( unusual_vol_boolean_df ) & ( vol_val_boolean_df )
        return result_boolean_df.rename( columns={ 'Volume': 'Unusual Vol' } )
    else:
        return unusual_vol_boolean_df.rename( columns={ 'Volume': 'Unusual Vol' } )

def get_upside_change_boolean_df( src_df: DataFrame, gap_up_pct: float, close_pct: float ) -> DataFrame:
    gap_up_pct_change_df = src_df.loc[ :, idx[ :, 'Gap Diff' ] ]
    close_pct_change_df = src_df.loc[ :, idx[ :, 'Pct Change' ] ]

    gap_diff_boolean_df = ( gap_up_pct_change_df > gap_up_pct ).rename( columns={ 'Gap Diff': 'Up' } )
    close_pct_change_boolean_df = ( close_pct_change_df >= close_pct ).rename( columns={ 'Pct Change': 'Up' } )

    if gap_up_pct != None and close_pct != None:
        return ( gap_diff_boolean_df ) & ( close_pct_change_boolean_df )
    elif gap_up_pct != None and close_pct == None:
        return gap_diff_boolean_df
    elif gap_up_pct == None and close_pct != None:
        return close_pct_change_boolean_df

def get_increasing_vol_boolean_df( src_df: DataFrame, increasing_vol_extent: float, vol_val: int, consecutive_day: int ):
    vol_df = src_df.loc[ :, idx[ :, 'Volume' ] ].rename( columns={ 'Volume': 'Increasing Vol' } )
    vol_pct_change_df = src_df.loc[ :, idx[ :, 'Vol Change' ] ].rename( columns={ 'Vol Change': 'Increasing Vol' } )

    if vol_val == None:
        vol_compare_df = ( vol_pct_change_df >= increasing_vol_extent ) & ( vol_df >= vol_val )
    else:
        vol_compare_df = ( vol_pct_change_df >= increasing_vol_extent )

    return get_consecutive_count_boolean_df( vol_compare_df, consecutive_day )
        
def derive_idx_df_from_src_df( src_df: DataFrame ) -> DataFrame:
    numeric_idx_value_list = src_df.reset_index( drop=True, level=0 ).reset_index().loc[ :, idx[ [ 'index' ], : ] ].values
    return pd.DataFrame( np.repeat( numeric_idx_value_list, len( src_df.columns ), axis=1 ), columns=src_df.columns ).rename( columns={ src_df.columns.get_level_values( 1 ).values[ 0 ]: 'Index' } )
    
def get_consecutive_count_boolean_df( src_boolean_df: DataFrame, consecutive_day: int ) -> DataFrame:
    consecutive_pct_change_day_df = src_boolean_df.cumsum() - src_boolean_df.cumsum().where( ~src_boolean_df ).ffill().fillna( 0 )
    consecutive_pct_change_day_sum_df = consecutive_pct_change_day_df.where( src_boolean_df.values ).ffill()
    consecutive_pct_change_day_sum_df = ( consecutive_pct_change_day_sum_df.where( ~src_boolean_df.values ).bfill() ).where( src_boolean_df.values )
    
    return ( consecutive_pct_change_day_sum_df >= consecutive_day )

def get_data_from_df_by_idx( src_data_df: DataFrame, src_idx_df: DataFrame ) -> DataFrame:
    idx_df = derive_idx_df_from_src_df( src_data_df )
    expand_src_idx_df = pd.concat( [ src_idx_df ] * len( src_data_df ) ).set_index( idx_df.index )
    select_index_boolean_df = ( expand_src_idx_df == idx_df )
    
    result_df = src_data_df.where( select_index_boolean_df.values ).fillna( method='bfill' ).iloc[ [ 0 ] ].reset_index( drop=True )
    return result_df.rename( index={ result_df.index.values[ 0 ]: 0 }, columns={ result_df.columns.get_level_values( 1 ).values[ 0 ]: 'Compare' } )

def get_data_from_df_by_idx_range( src_data_df: DataFrame, src_start_idx_df: DataFrame, max_consolidation_range: int ) -> DataFrame:
    idx_df = derive_idx_df_from_src_df( src_data_df )
    expand_src_start_idx_df = pd.concat( [ src_start_idx_df ] * len( src_data_df ) ).set_index( idx_df.index )

    if max_consolidation_range != None:
        unusual_vol_and_upside_end_idx_df = src_start_idx_df.add( max_consolidation_range )
        expand_src_end_idx_df = pd.concat( [ unusual_vol_and_upside_end_idx_df ] * len( src_data_df ) ).set_index( idx_df.index )
        idx_boolean_df = ( idx_df >= expand_src_start_idx_df ) & ( idx_df <= expand_src_end_idx_df )
    else:
        idx_boolean_df = ( idx_df >= expand_src_start_idx_df )

    return src_data_df.where( idx_boolean_df.values )
    
def get_consolidation_df( 
            historical_data_df: DataFrame,
            start_idx: Union[int, DataFrame],
            consolidation_tolerance: float, consolidation_indicators: list, count_mode: str,
            min_consolidation_range: int, max_consolidation_range: int ) -> DataFrame:
    min_tolerance = 1 - ( consolidation_tolerance/ 100 )
    max_tolerance = 1 + ( consolidation_tolerance/ 100 )

    if isinstance(start_idx, DataFrame):
        src_high_df = get_data_from_df_by_idx_range( historical_data_df.loc[ :, idx[ :, 'High' ] ], start_idx, max_consolidation_range )
        src_low_df = get_data_from_df_by_idx_range( historical_data_df.loc[ :, idx[ :, 'Low' ] ], start_idx, max_consolidation_range )
        src_close_df = get_data_from_df_by_idx_range( historical_data_df.loc[ :, idx[ :, 'Close' ] ], start_idx, max_consolidation_range )
    elif isinstance(start_idx, int):
        src_high_df = historical_data_df.loc[ :, idx[ :, 'High' ] ]
        src_low_df = historical_data_df.loc[ :, idx[ :, 'Low' ] ]
        src_close_df = historical_data_df.loc[ :, idx[ :, 'Close' ] ]

    repeat_src_high_df = pd.DataFrame( np.repeat( src_high_df.values, len( src_high_df ), axis=0 ), columns=src_high_df.columns, index=np.repeat( src_high_df.reset_index().index.tolist(), len( src_high_df ), axis=0 ) ).rename( columns={ 'High': 'Compare' } )
    expand_src_high_df = pd.concat( [ src_high_df ] * len( src_high_df ) ).rename( columns={ 'High': 'Compare' } )
    min_high_df = expand_src_high_df.mul( min_tolerance ).set_index( repeat_src_high_df.index )
    max_high_df = expand_src_high_df.mul( max_tolerance ).set_index( repeat_src_high_df.index )

    repeat_src_low_df = pd.DataFrame( np.repeat( src_low_df.values, len( src_low_df ), axis=0 ), columns=src_low_df.columns, index=np.repeat( src_low_df.reset_index().index.tolist(), len( src_low_df ), axis=0 ) ).rename( columns={ 'Low': 'Compare' } )
    expand_src_low_df = pd.concat( [ src_low_df ] * len( src_low_df ) ).rename( columns={ 'Low': 'Compare' } )
    min_low_df = expand_src_low_df.mul( min_tolerance ).set_index( repeat_src_low_df.index )
    max_low_df = expand_src_low_df.mul( max_tolerance ).set_index( repeat_src_low_df.index )

    repeat_src_close_df = pd.DataFrame( np.repeat( src_close_df.values, len( src_close_df ), axis=0 ), columns=src_close_df.columns, index=np.repeat( src_close_df.reset_index().index.tolist(), len( src_close_df ), axis=0 ) ).rename( columns={ 'Close': 'Compare' } )
    expand_src_close_df = pd.concat( [ src_close_df ] * len( src_close_df ) ).rename( columns={ 'Close': 'Compare' } )
    min_close_df = expand_src_close_df.mul( min_tolerance ).set_index( repeat_src_close_df.index )
    max_close_df = expand_src_close_df.mul( max_tolerance ).set_index( repeat_src_close_df.index )

    repeat_idx_df = pd.concat( [ pd.DataFrame( repeat_src_low_df.index ) ] * len( repeat_src_low_df.columns ), axis=1 )
    repeat_idx_boolean_df = ( ~repeat_idx_df.diff().fillna( 1 ).astype( bool ) )

    high_in_range_boolean_df = ( repeat_src_high_df >= min_high_df ) & ( repeat_src_high_df <= max_high_df )
    low_in_range_boolean_df = ( repeat_src_low_df >= min_low_df ) & ( repeat_src_low_df <= max_low_df )
    close_in_range_boolean_df = ( repeat_src_close_df >= min_close_df ) & ( repeat_src_close_df <= max_close_df )

    if count_mode == 'CONSECUTIVE':
        high_in_range_boolean_count_df = get_consecutive_count_boolean_df( high_in_range_boolean_df.where( repeat_idx_boolean_df.values ).fillna( False ), min_consolidation_range )
        low_in_range_boolean_count_df = get_consecutive_count_boolean_df( low_in_range_boolean_df.where( repeat_idx_boolean_df.values ).fillna( False ), min_consolidation_range )
        close_in_range_boolean_count_df = get_consecutive_count_boolean_df( close_in_range_boolean_df.where( repeat_idx_boolean_df.values ).fillna( False ), min_consolidation_range )
    elif count_mode == 'SUM':
        high_in_range_boolean_count_df = ( high_in_range_boolean_df.groupby( high_in_range_boolean_df.index ).sum() >= min_consolidation_range )
        low_in_range_boolean_count_df = ( low_in_range_boolean_df.groupby( low_in_range_boolean_df.index ).sum() >= min_consolidation_range )
        close_in_range_boolean_count_df = ( close_in_range_boolean_df.groupby( low_in_range_boolean_df.index ).sum() >= min_consolidation_range )
    
    indicator_to_in_range_boolean_df_dict = {
        'High': high_in_range_boolean_count_df,
        'Low': low_in_range_boolean_count_df,
        'Close': close_in_range_boolean_count_df
    }

    in_range_boolean_df_list = []
    
    for indicator in consolidation_indicators:
        in_range_boolean_df_list.append( indicator_to_in_range_boolean_df_dict[ indicator ] )

    for index, in_range_boolean_df in enumerate(in_range_boolean_df_list):
        if index == 0:
            result_boolean_df = in_range_boolean_df
        else:
            result_boolean_df = ( result_boolean_df ) | ( in_range_boolean_df )

    result_boolean_df = pd.DataFrame( result_boolean_df.any() ).T
    return result_boolean_df
