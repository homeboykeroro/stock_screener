from numpy import array, nan
import pandas as pd
import logging
import time
import os
import json
import math

from utils.stock_data_utils import *
from utils.filter_utils import *

historical_data_df_list = []
idx = pd.IndexSlice

def filter_by_unfilled_gap_up( 
            unusual_upside_indicators=[ { 'type': 'LONG_UPPER_SHADOW', 'shadowCandlestickRatio': 58, 'gapUpPct': 1 }, 
                                    { 'type': 'MARUBOZU', 'color': 'RED', 'marubozuRatio': 58, 'higherHighPct': 1 } ],
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
            
            gap_fill_value_df = get_data_from_df_by_idx( high_df, unusual_vol_and_upside_idx_df.sub( 1 ) ).rename( columns={ 'High': 'Compare' } )
            current_low_df = low_df.iloc[ [ -1 ] ].rename( columns={ 'Low': 'Compare' } ).reset_index( drop=True )

            if gap_fill_tolerance != None:
                min_multiplier = 1 + ( gap_fill_tolerance/ 100 )
                gap_fill_value_df = gap_fill_value_df.mul( min_multiplier )

            filled_gap_range_boolean_df = ( current_low_df >= gap_fill_value_df )
            result_boolean_df = ( min_observe_day_boolean_df ) & ( filled_gap_range_boolean_df )
            
            ticker_to_filtered_result_series = result_boolean_df.any()
            result_ticker_list.extend( ticker_to_filtered_result_series.index[ ticker_to_filtered_result_series ].get_level_values( 0 ).tolist() )

        log_msg( "--- Filter By Unfill Gap Time, %s seconds ---" % ( time.time() - start_time ) )
    except Exception as e:
        logging.exception( 'Filter By Unfill Gap Failed, Cause: %s' % e )
        raise Exception( 'Filter By Unfill Gap Error' )

    return result_ticker_list

def filter_by_consolidation_after_momentum( 
            unusual_upside_indicators=[ { 'type': 'LONG_UPPER_SHADOW', 'shadowCandlestickRatio': 58, 'gapUpPct': 1 }, 
                                    { 'type': 'MARUBOZU', 'color': 'RED', 'marubozuRatio': 58, 'higherHighPct': 1 } ],
            compare_unusual_vol_ma=50, unusual_vol_extent=150, unusual_vol_val=300000, 
            unusual_vol_and_upside_occurrence='LAST',
            consolidation_tolerance=4, consolidation_indicators=[ 'Low', 'High', 'Close' ], 
            consolidation_indicators_compare='AND', count_mode='CONSECUTIVE',
            min_consolidation_range=4, max_consolidation_range=13,
            min_consolidation_range_pct=None,
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
            
            min_consolidation_range = math.floor( day_period * ( min_consolidation_range_pct/ 100 ) ) if min_consolidation_range == None else min_consolidation_range
            max_consolidation_range = day_period if max_consolidation_range == None else max_consolidation_range

            consolidation_boolean_df = get_consolidation_df( historical_data_df,
                                                            unusual_vol_and_upside_idx_df,
                                                            consolidation_tolerance, consolidation_indicators, 
                                                            consolidation_indicators_compare, count_mode,
                                                            min_consolidation_range, max_consolidation_range ).rename( columns={ 'Consolidation': 'Compare' } )

            result_boolean_df = ( min_consolidation_range_boolean_df ) & ( consolidation_boolean_df )

            ticker_to_filtered_result_series = result_boolean_df.any()
            result_ticker_list.extend( ticker_to_filtered_result_series.index[ ticker_to_filtered_result_series ].get_level_values( 0 ).tolist() )

        log_msg( '"--- Filter By Consolidation After Uptrend Momentum, %s seconds "---' % ( time.time() - start_time ) )
    except Exception as e:
        logging.exception( 'Filter By Consolidation After Uptrend Momentum Failed, Cause: %s' % e  )
        raise Exception( 'Filter By Consolidation After Uptrend Momentum Error' )

    return result_ticker_list

def filter_by_bullish_reversal( 
            unusual_upside_indicators=[ { 'type': 'LONG_LOWER_SHADOW', 'shadowCandlestickRatio': 50 }, 
                                    { 'type': 'MARUBOZU', 'color': 'GREEN', 'marubozuRatio': 50 } ],
            compare_unusual_vol_ma=20, unusual_vol_extent=120, unusual_vol_val=300000, 
            unusual_vol_and_upside_occurrence='FIRST',
            all_time_low_observe_period=None,
            day_period=20, **kwargs ):
    start_time = time.time()
    result_ticker_list = []

    try:
        for index, historical_data_df in enumerate( historical_data_df_list ):
            low_df = historical_data_df.loc[ :, idx[ :, 'Low' ] ]
            close_df = historical_data_df.loc[ :, idx[ :, 'Close' ] ]

            if all_time_low_observe_period != None:
                low_df = select_data_by_time_period( low_df, all_time_low_observe_period )
                close_df = select_data_by_time_period( close_df, all_time_low_observe_period )
            else:
                all_time_low_observe_period = len( historical_data_df )
            
            last_occurrence_new_low_idx_df = ( pd.DataFrame( low_df.reset_index( drop=True ).idxmin() ).T ).rename( columns={ 'Low': 'Compare' } )
            last_occurrence_new_low_close_idx_df = ( pd.DataFrame( close_df.reset_index( drop=True ).idxmin() ).T ).rename( columns={ 'Close': 'Compare' } )

            new_low_period_range_df = last_occurrence_new_low_idx_df.apply( lambda x: all_time_low_observe_period - x )
            new_low_close_period_range_df = last_occurrence_new_low_close_idx_df.apply( lambda x: all_time_low_observe_period - x )

            historical_data_df = select_data_by_time_period( historical_data_df, day_period )

            unusual_vol_and_upside_idx_df = get_unusual_vol_and_upside_idx_df( historical_data_df,
                                                                                unusual_upside_indicators, 
                                                                                compare_unusual_vol_ma, unusual_vol_extent, unusual_vol_val,
                                                                                unusual_vol_and_upside_occurrence )
            
            unusual_vol_and_upside_idx_df = unusual_vol_and_upside_idx_df.apply( lambda x: day_period - x ).rename( columns={ 'Index': 'Compare' } )
            day_period_df = pd.DataFrame( np.repeat( [ [ day_period ] ], len( unusual_vol_and_upside_idx_df.columns ), axis=1 ), columns=unusual_vol_and_upside_idx_df.columns, index=unusual_vol_and_upside_idx_df.index  )

            reversal_after_new_low_boolean_df = ( unusual_vol_and_upside_idx_df <= new_low_period_range_df ) & ( new_low_period_range_df <= day_period_df )
            reversal_after_new_low_close_boolean_df = ( unusual_vol_and_upside_idx_df <= new_low_close_period_range_df ) & ( new_low_close_period_range_df <= day_period_df )
            result_boolean_df = ( reversal_after_new_low_boolean_df ) | ( reversal_after_new_low_close_boolean_df )

            ticker_to_filtered_result_series = result_boolean_df.any()
            result_ticker_list.extend( ticker_to_filtered_result_series.index[ ticker_to_filtered_result_series ].get_level_values( 0 ).tolist() )

        log_msg( "--- Filter By Bullish Reversal Time, %s seconds ---" % ( time.time() - start_time ) )
    except Exception as e:
        logging.exception( 'Filter By Bullish Reversal Failed, Cause: %s' % e )
        raise Exception( 'Filter By Bullish Reversal Error' )

    return result_ticker_list

def filter_by_weinstein_stage_one_accumulation( 
            vol_val=300000,
            count_mode='SUM', consolidation_tolerance=5, compare_ma_tolerance=12, 
            day_period=3, **kwargs ):
    start_time = time.time()
    result_ticker_list = []

    try:
        for index, historical_data_df in enumerate( historical_data_df_list ):
            historical_data_df = select_data_by_time_period( historical_data_df, day_period )

            high_df = historical_data_df.loc[ :, idx[ :, 'High' ] ].rename( columns={ 'High': 'Compare' } )
            low_df = historical_data_df.loc[ :, idx[ :, 'Low' ] ].rename( columns={ 'Low': 'Compare' } )
            close_df = historical_data_df.loc[ :, idx[ :, 'Close' ] ].rename( columns={ 'Close': 'Compare' } )
            vol_df = historical_data_df.loc[ :, idx[ :, 'Volume' ] ].rename( columns={ 'Volume': 'Compare' } )
            ma_200_close_df = historical_data_df.loc[ :, idx[ :, '200MA Close' ] ].rename( columns={ '200MA Close': 'Compare' } )

            range_multiplier = compare_ma_tolerance/ 100
            min_ma_200_close_range_df = ma_200_close_df.mul( 1 - range_multiplier )
            max_ma_200_close_range_df = ma_200_close_df.mul( 1 + range_multiplier )

            high_in_200_ma_tolerance_range_boolean_df = ( high_df >= min_ma_200_close_range_df ) & ( high_df <= max_ma_200_close_range_df )
            low_in_200_ma_tolerance_range_boolean_df = ( low_df >= min_ma_200_close_range_df ) & ( low_df <= max_ma_200_close_range_df )
            close_in_200_ma_tolerance_range_boolean_df = ( close_df >= min_ma_200_close_range_df ) & ( close_df <= max_ma_200_close_range_df )
            
            hls_in_200_ma_tolerance_range_boolean_df = pd.DataFrame( ( ( high_in_200_ma_tolerance_range_boolean_df.all() ) | ( low_in_200_ma_tolerance_range_boolean_df.all() ) | ( close_in_200_ma_tolerance_range_boolean_df.all() ) ) ).T
            vol_val_boolean_df = pd.DataFrame( ( vol_df.gt( vol_val ) ).all() ).T

            consolidation_boolean_df = get_consolidation_df( historical_data_df,
                                                            day_period,
                                                            consolidation_tolerance, ['High', 'Low', 'Close'], 
                                                            'OR', count_mode,
                                                            day_period, day_period ).rename( columns={ 'Consolidation': 'Compare' } )

            result_boolean_df = ( hls_in_200_ma_tolerance_range_boolean_df ) & ( vol_val_boolean_df ) & ( consolidation_boolean_df )
            ticker_to_filtered_result_series = result_boolean_df.any()
            result_ticker_list.extend( ticker_to_filtered_result_series.index[ ticker_to_filtered_result_series ].get_level_values( 0 ).tolist() )

        log_msg( "--- Filter By Weinstein Stage One Accumulation Time, %s seconds ---" % ( time.time() - start_time ) )
    except Exception as e:
        logging.exception( 'Filter By Weinstein Stage One Accumulation Failed, Cause: %s' % e )
        raise Exception( 'Filter By Weinstein Stage One Accumulation Error' )

    return result_ticker_list

def filter_by_previous_resistance_test( 
            unusual_upside_indicators=[ { 'type': 'LONG_UPPER_SHADOW', 'shadowCandlestickRatio': 60, 'bodyGapUpPct': 5, 'closePct': 10 } ],
            compare_unusual_vol_ma=50, unusual_vol_extent=160, unusual_vol_val=500000,
            unusual_vol_and_upside_occurrence=None,
            tolerance=5, unusual_vol_and_upside_resistance_indicators=['Low', 'Close'], resistance_test_indicators=['High', 'Low', 'Close'],
            day_period=10, **kwargs ):
    start_time = time.time()
    result_ticker_list = []

    try:
        for index, historical_data_df in enumerate( historical_data_df_list ):
            unusual_vol_and_upside_idx_df = get_unusual_vol_and_upside_idx_df( historical_data_df,
                                                                                unusual_upside_indicators, 
                                                                                compare_unusual_vol_ma, unusual_vol_extent, unusual_vol_val,
                                                                                unusual_vol_and_upside_occurrence )
            
            unusual_vol_and_upside_high_df = get_data_from_df_by_idx( historical_data_df.loc[ :, idx[ :, 'High' ] ] , unusual_vol_and_upside_idx_df )
            unusual_vol_and_upside_low_df = get_data_from_df_by_idx( historical_data_df.loc[ :, idx[ :, 'Low' ] ], unusual_vol_and_upside_idx_df )
            unusual_vol_and_upside_close_df = get_data_from_df_by_idx( historical_data_df.loc[ :, idx[ :, 'Close' ] ], unusual_vol_and_upside_idx_df )

            high_resistance_ref_val = get_same_size_arry_elememt_np( unusual_vol_and_upside_high_df.values.T )
            low_resistance_ref_val = get_same_size_arry_elememt_np( unusual_vol_and_upside_low_df.values.T )
            close_resistance_ref_val = get_same_size_arry_elememt_np( unusual_vol_and_upside_close_df.values.T )
            
            ticker_list = historical_data_df.loc[ :, idx[ :, 'High' ] ].columns.get_level_values( 0 ).values.tolist()
            high_resistance_ref_val_col_list = [ 'High Resistance ' + str( elem ) for elem in np.arange( high_resistance_ref_val[ 0 ].size ).tolist() ]
            low_resistance_ref_val_col_list = [ 'Low Resistance ' + str( elem ) for elem in np.arange( low_resistance_ref_val[ 0 ].size ).tolist() ]
            close_resistance_ref_val_col_list = [ 'Close Resistance ' + str( elem ) for elem in np.arange( close_resistance_ref_val[ 0 ].size ).tolist() ]
            
            historical_data_df = select_data_by_time_period( historical_data_df, day_period )
            high_in_day_period_df = historical_data_df.loc[ :, idx[ :, 'High' ] ].reset_index( drop=True ).rename( columns={ 'High': 'Compare' } )
            low_in_day_period_df = historical_data_df.loc[ :, idx[ :, 'Low' ] ].reset_index( drop=True ).rename( columns={ 'Low': 'Compare' } )
            close_in_day_period_df = historical_data_df.loc[ :, idx[ :, 'Close' ] ].reset_index( drop=True ).rename( columns={ 'Close': 'Compare' } )

            high_resistance_ref_df = pd.DataFrame( np.array( [ high_resistance_ref_val.ravel().tolist() ] * day_period ),
                                                    columns=pd.MultiIndex.from_product( [ ticker_list, high_resistance_ref_val_col_list ] ),
                                                    index=np.arange( day_period ) )
            low_resistance_ref_df = pd.DataFrame( np.array( [ low_resistance_ref_val.ravel().tolist() ] * day_period ),
                                                    columns=pd.MultiIndex.from_product( [ ticker_list, low_resistance_ref_val_col_list ] ),
                                                    index=np.arange( day_period ) )
            close_resistance_ref_df = pd.DataFrame( np.array( [ close_resistance_ref_val.ravel().tolist() ] * day_period ),
                                                    columns=pd.MultiIndex.from_product( [ ticker_list, close_resistance_ref_val_col_list ] ),
                                                    index=np.arange( day_period ) )

            indicator_to_resistance_ref_df_dict = {
                'High': high_resistance_ref_df,
                'Low': low_resistance_ref_df,
                'Close': close_resistance_ref_df
            }

            indicator_to_data_in_day_period_df_dict = {
                'High': high_in_day_period_df,
                'Low': low_in_day_period_df,
                'Close': close_in_day_period_df
            }

            for unusual_vol_and_upside_resistance_indicator in unusual_vol_and_upside_resistance_indicators:
                for resistance_ref_val_idx in range( len( high_resistance_ref_val_col_list ) ):
                    tolerance_multiplier = 1/ tolerance
                    high_resistance_ref_val_col = high_resistance_ref_val_col_list[ resistance_ref_val_idx ]
                    low_resistance_ref_val_col = low_resistance_ref_val_col_list[ resistance_ref_val_idx ]
                    close_resistance_ref_val_col = close_resistance_ref_val_col_list[ resistance_ref_val_idx ]

                    if unusual_vol_and_upside_resistance_indicator == 'High':
                        resistance_ref_val_col = high_resistance_ref_val_col
                    elif unusual_vol_and_upside_resistance_indicator == 'Low':
                        resistance_ref_val_col = low_resistance_ref_val_col
                    elif unusual_vol_and_upside_resistance_indicator == 'Close':
                        resistance_ref_val_col = close_resistance_ref_val_col

                    resistance_ref_min_range_df = indicator_to_resistance_ref_df_dict.get( unusual_vol_and_upside_resistance_indicator ).loc[ :, idx[ :, resistance_ref_val_col ] ].mul( 1 - tolerance_multiplier ).rename( columns={ resistance_ref_val_col: 'Compare' } )
                    resistance_ref_max_range_df = indicator_to_resistance_ref_df_dict.get( unusual_vol_and_upside_resistance_indicator ).loc[ :, idx[ :, resistance_ref_val_col ] ].mul( 1 + tolerance_multiplier ).rename( columns={ resistance_ref_val_col: 'Compare' } )

                    for resistance_test_indicator_idx, resistance_test_indicator in enumerate( resistance_test_indicators ):
                        resistance_test_data_df = indicator_to_data_in_day_period_df_dict.get( resistance_test_indicator )

                        if resistance_test_indicator_idx == 0:
                            result_boolean_df = ( resistance_test_data_df >= resistance_ref_min_range_df ) & ( resistance_test_data_df <= resistance_ref_max_range_df )
                        else:
                            result_boolean_df = ( result_boolean_df ) | ( ( resistance_test_data_df >= resistance_ref_min_range_df ) & ( resistance_test_data_df <= resistance_ref_max_range_df ) )

            ticker_to_filtered_result_series = result_boolean_df.all()
            result_ticker_list.extend( ticker_to_filtered_result_series.index[ ticker_to_filtered_result_series ].get_level_values( 0 ).tolist() )

        log_msg( "--- Filter By Previous Resistance Test Time, %s seconds ---" % ( time.time() - start_time ) )
    except Exception as e:
        logging.exception( 'Filter By Previous Resistance Test Failed, Cause: %s' % e )
        raise Exception( 'Filter By Previous Resistance Test Error' )

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
                'bullish_reversal': filter_by_bullish_reversal,
                'weinstein_stage_one_accumulation': filter_by_weinstein_stage_one_accumulation,
                'previous_resistance_test': filter_by_previous_resistance_test
            }

            for filter_condition_dict in filter_condition_dict_list:
                filtered_ticker_list.append( operation_dict.get( filter_condition_dict[ 'operation' ], [] )( **filter_condition_dict ) )
                result = set( filtered_ticker_list[ 0 ] ).intersection( *filtered_ticker_list[ 1: ] )
                
                get_stock_chart_by_ticker_list( result, filter_condition_dict_list )
        except Exception as e:
            logging.exception( 'Get Filter Result Failed, Cause: %s' % e )

filter_stocks()