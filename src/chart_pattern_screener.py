import pandas as pd
import logging
import time
import os
import json

from utils.stock_data_utils import *
from utils.filter_utils import *

historical_data_df_list = []
idx = pd.IndexSlice

def filter_by_unfilled_gap_up( 
            unusual_upside_indicators=[ { 'type': 'LONG_UPPER_SHADOW', 'shadowCandlestickRatio': 58, 'gapUpPct': 1 }, 
                                    { 'type': 'MARUBOZU', 'color': 'RED', 'marubozu_ratio': 58, 'higherHighPct': 1 } ],
            compare_unusual_vol_ma=50, unusual_vol_extent=150, unusual_vol_val=300000, 
            unusual_vol_and_upside_occurrence='FIRST',
            min_observe_day=4, gap_fill_tolerance=None, 
            day_period=35, **kwargs ):
    start_time = time.time()
    result_ticker_list = []

    try:
        for index, historical_data_df in enumerate( historical_data_df_list ):
            historical_data_df = select_data_by_time_period( historical_data_df, day_period )

            high_df = historical_data_df.loc[ :, idx[ :, 'High' ] ]
            low_df = historical_data_df.loc[ :, idx[ :, 'Low' ] ]

            unusual_vol_and_upside_idx_df = get_unusual_vol_and_upside_idx_df( historical_data_df,
                                                                                unusual_upside_indicators, 
                                                                                compare_unusual_vol_ma, unusual_vol_extent, unusual_vol_val,
                                                                                unusual_vol_and_upside_occurrence )

            min_observe_day_df = unusual_vol_and_upside_idx_df.apply( lambda x : day_period - x )
            min_observe_day_boolean_df = ( min_observe_day_df >= min_observe_day ).rename( columns={ 'Index': 'Compare' } )
            
            gap_fill_value_df = get_data_from_df_by_idx( high_df, unusual_vol_and_upside_idx_df.sub( 1 ) )
            current_low_df = low_df.iloc[ [ -1 ] ].rename( columns={ 'Low': 'Compare' } ).reset_index( drop=True )

            if gap_fill_tolerance != None:
                min_multiplier = 1 + ( gap_fill_tolerance/ 100 )
                gap_fill_value_df = gap_fill_value_df.mul( min_multiplier )

            filled_gap_range_boolean_df = ( current_low_df >= gap_fill_value_df )
            result_boolean_df = ( min_observe_day_boolean_df ) & ( filled_gap_range_boolean_df )
            
            ticker_to_filtered_result_series = result_boolean_df.any()
            result_ticker_list.extend( ticker_to_filtered_result_series.index[ ticker_to_filtered_result_series ].get_level_values( 0 ).tolist() )

        log_msg( "--- Filter By Unfill Gap Pattern Time, %s seconds ---" % ( time.time() - start_time ) )
    except Exception as e:
        logging.exception( 'Filter By Unfill Gap Pattern Failed, Cause: %s' % e )
        raise Exception( 'Filter By Unfill Gap Pattern Error' )

    return result_ticker_list

def filter_by_consolidation_after_momentum( 
            unusual_upside_indicators=[ { 'type': 'LONG_UPPER_SHADOW', 'shadowCandlestickRatio': 58, 'gapUpPct': 1 }, 
                                    { 'type': 'MARUBOZU', 'color': 'RED', 'marubozu_ratio': 58, 'higherHighPct': 1 } ],
            compare_unusual_vol_ma=50, unusual_vol_extent=150, unusual_vol_val=300000, 
            unusual_vol_and_upside_occurrence='LAST',
            consolidation_tolerance=4, consolidation_indicators=[ 'Low', 'High', 'Close' ], 
            consolidation_indicators_compare='AND', count_mode='CONSECUTIVE',
            min_consolidation_range=4, max_consolidation_range=13,
            day_period=35, **kwargs ):
    start_time = time.time()
    result_ticker_list = []

    try:
        for index, historical_data_df in enumerate( historical_data_df_list ):
            historical_data_df = select_data_by_time_period( historical_data_df, day_period )

            unusual_vol_and_upside_idx_df = get_unusual_vol_and_upside_idx_df( historical_data_df,
                                                                                unusual_upside_indicators, 
                                                                                compare_unusual_vol_ma, unusual_vol_extent, unusual_vol_val,
                                                                                unusual_vol_and_upside_occurrence )

            min_consolidation_range_df = unusual_vol_and_upside_idx_df.apply( lambda x : day_period - x )
            min_consolidation_range_boolean_df = ( min_consolidation_range_df >= min_consolidation_range ).rename( columns={ 'Index': 'Compare' } )
            
            consolidation_boolean_df = get_consolidation_df( historical_data_df,
                                                            unusual_vol_and_upside_idx_df,
                                                            consolidation_tolerance, consolidation_indicators, 
                                                            consolidation_indicators_compare, count_mode,
                                                            min_consolidation_range, max_consolidation_range )

            result_boolean_df = ( min_consolidation_range_boolean_df ) & ( consolidation_boolean_df )

            ticker_to_filtered_result_series = result_boolean_df.any()
            result_ticker_list.extend( ticker_to_filtered_result_series.index[ ticker_to_filtered_result_series ].get_level_values( 0 ).tolist() )

        log_msg( '"--- Filter By Consolidation After Uptrend Momentum, %s seconds "---' % ( time.time() - start_time ) )
    except Exception as e:
        logging.exception( 'Filter By Consolidation After Uptrend Momentum Failed, Cause: %s' % e  )
        raise Exception( 'Filter By Consolidation After Uptrend Momentum Error' )

    return result_ticker_list

def filter_stocks():
    global historical_data_df_list
    root_dir = config[ 'STOCK_DATA_ROOT_FOLDER_DIR' ]

    if DataSourceType.STOOQ.name == historical_data_source_type:
        historical_data_folder_dir = os.path.join( root_dir, 'Historical/Stooq/Full/' )
    elif DataSourceType.YFINANCE.name == historical_data_source_type:
        historical_data_folder_dir = os.path.join( root_dir, 'Historical/Yfinance/Full/' )

    historical_data_df_list = load_src_data_dataframe( [ historical_data_folder_dir ] )

    while True:
        try:
            filter_condition_json = input( 'Input Filter Condition: ' )
            filter_condition_dict_list = json.loads( filter_condition_json )
            filtered_ticker_list = []

            operation_dict = {
                'unfilled_gap_up': filter_by_unfilled_gap_up,
                'consolidation_after_uptrend_momentum': filter_by_consolidation_after_momentum,
            }

            for filter_condition_dict in filter_condition_dict_list:
                filtered_ticker_list.append( operation_dict.get( filter_condition_dict[ 'operation' ], [] )( **filter_condition_dict ) )

            result = set( filtered_ticker_list[ 0 ] ).intersection( *filtered_ticker_list[ 1: ] )
            get_stock_chart_by_ticker_list( result, filter_condition_dict_list )
        except Exception as e:
            logging.exception( 'Get Filter Result Failed, Cause: %s' % e )

filter_stocks()