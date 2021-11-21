import time
import datetime
import os
import pandas as pd
import numpy as np

from config.config import config
from constant.filter_criteria import FilterCriteria
from constant.indicator.indicator import Indicator
from constant.indicator.customised_indicator import CustomisedIndicator
from constant.indicator.runtime_indicator import RuntimeIndicator
from constant.candle.candle_colour import CandleColour

from utils.log_util import get_logger

logger = get_logger()

root_folder_dir = config['STOCK_DATA_ROOT_FOLDER_DIR']
idx = pd.IndexSlice

def get_stock_chart(ticker_list: list, filter_criteria_dict: list) -> None:
    logger.debug('Number of Stock Charts: %s' % len(ticker_list))
    url_list = []

    for ticker in ticker_list:
        url = f'https://finviz.com/chart.ashx?t={ticker}&ty=c&ta=1&p=d'
        url_list.append(url)

    try:
        current_date = datetime.datetime.now().strftime('%Y%m%d')
        current_time = datetime.datetime.now().strftime('%H%M%S')
        export_folder_dir = os.path.join(root_folder_dir, f'Chart/{current_date}{current_time}_{filter_criteria_dict.get(FilterCriteria.PATTERN)}/')
        os.makedirs(export_folder_dir)

        header_html_str = f'<div style="font-size:23px; font-weight: bold; margin-bottom: 50px;">No. of Filtered Result: {str(len(ticker_list))}</div>'
        
        export_file_dir = os.path.join(export_folder_dir, 'chart.html')
        with open(export_file_dir, 'w') as chart_file:
            chart_file.write(header_html_str)

            for url in url_list:
                chart_file.write(f'<img style="width:900px; height:300px" src=\"{url}\"/>\n')

            chart_file.write(f'<div>{filter_criteria_dict}</div>')
    except Exception as e:
        raise e

def load_historical_data_into_df(folder_dir_list: str) -> list:
    start_time = time.time()
    data_file_dir_list = []
    df_list = []

    try:
        for folder_dir in folder_dir_list:
            dir_list = [os.path.join(folder_dir, data_file_dir) for data_file_dir in os.listdir(folder_dir) if os.path.isfile(os.path.join(folder_dir, data_file_dir))]
            data_file_dir_list.extend(dir_list)
        
        df_list = [pd.read_csv(dir, header=[0, 1], index_col=0) for dir in data_file_dir_list]
        logger.debug(f'Historical Data Loading Time, {(time.time() - start_time)} seconds')
    except Exception as e:
        raise e
    
    return df_list

def append_custom_indicators(folder_dir_list: list, export_folder_dir: str) -> list:
    start_time = time.time()
    historical_data_df_list = load_historical_data_into_df(folder_dir_list)
    customised_historical_data_df_list = []

    try:
        for index, historical_data_df in enumerate(historical_data_df_list):
            append_start_time = time.time()

            high_df = historical_data_df.loc[:, idx[:, Indicator.HIGH]]
            low_df = historical_data_df.loc[:, idx[:, Indicator.LOW]]
            close_df = historical_data_df.loc[:, idx[:, Indicator.CLOSE]]
            volume_df = historical_data_df.loc[:, idx[:, Indicator.VOLUME]]

            compare_close_df = historical_data_df.loc[:, idx[:, Indicator.CLOSE]].rename(columns={Indicator.CLOSE: RuntimeIndicator.COMPARE})
            compare_open_df = historical_data_df.loc[:, idx[:, Indicator.OPEN]].rename(columns={Indicator.OPEN: RuntimeIndicator.COMPARE})

            close_change_df = close_df.pct_change().mul(100).rename(columns={Indicator.CLOSE: CustomisedIndicator.CLOSE_CHANGE})
            high_change_df = high_df.pct_change().mul(100).rename(columns={Indicator.HIGH: CustomisedIndicator.HIGH_CHANGE})
            vol_change_df = volume_df.pct_change().mul(100).rename(columns={Indicator.VOLUME: CustomisedIndicator.VOLUME_CHANGE})
            sma_50_volume_df = volume_df.rolling(window=50).mean().rename(columns={Indicator.VOLUME: CustomisedIndicator.MA_50_VOLUME})

            #Normal Gap
            previois_close_df = compare_close_df.shift()
            gap_pct_df = ((compare_open_df.sub(previois_close_df.values)).div(previois_close_df.values)).mul(100).rename(columns={RuntimeIndicator.COMPARE: CustomisedIndicator.NORMAL_GAP})

            #Colour
            close_above_open_boolean_df = (compare_close_df > compare_open_df)
            colour_df = close_above_open_boolean_df.replace({True: CandleColour.GREEN, False: CandleColour.RED}).rename(columns={RuntimeIndicator.COMPARE: CustomisedIndicator.CANDLE_COLOUR})

            #Upper Shadow/ Lower Shadow/ Body Ratio
            high_low_diff_df = high_df.sub(low_df.values).rename(columns={Indicator.HIGH: RuntimeIndicator.COMPARE})
            close_above_open_upper_body_df = compare_close_df.where(close_above_open_boolean_df.values)
            open_above_close_upper_body_df = compare_open_df.where((~close_above_open_boolean_df).values)
            upper_body_df = close_above_open_upper_body_df.fillna(open_above_close_upper_body_df)

            open_below_close_lower_body_df = compare_open_df.where(close_above_open_boolean_df.values)
            close_below_open_lower_body_df = compare_close_df.where((~close_above_open_boolean_df).values)
            lower_body_df = open_below_close_lower_body_df.fillna(close_below_open_lower_body_df)
            
            body_diff_df = upper_body_df.sub(lower_body_df.values).replace(0, np.nan)
            body_ratio_df = (body_diff_df.div(high_low_diff_df.values)).mul(100).rename(columns={RuntimeIndicator.COMPARE: CustomisedIndicator.CANDLE_BODY_RATIO})
            upper_shadow_ratio_df = ((high_df.sub(upper_body_df.values).replace(0, np.nan)).div(high_low_diff_df.values)).mul(100).rename(columns={Indicator.HIGH: CustomisedIndicator.CANDLE_UPPER_SHADOW_RATIO})
            lower_shadow_ratio_df = ((lower_body_df.sub(low_df.values).replace(0, np.nan)).div(high_low_diff_df.values)).mul(100).rename(columns={RuntimeIndicator.COMPARE: CustomisedIndicator.CANDLE_LOWER_SHADOW_RATIO})

            previous_upper_body_df = upper_body_df.shift()
            previous_lower_body_df = lower_body_df.shift()

            #Candle Body Gap
            lower_body_above_previous_upper_body_boolean_df = (lower_body_df > previous_upper_body_df)
            previous_lower_body_above_upper_body_boolean_df = (previous_lower_body_df > upper_body_df)

            lower_body_previous_upper_body_pct_df = ((lower_body_df.sub(previous_upper_body_df.values)).div(previous_upper_body_df.values)).mul(100)
            upper_body_previous_lower_body_pct_df = ((upper_body_df.sub(previous_lower_body_df.values)).div(previous_lower_body_df.values)).mul(100)

            positive_body_gap_df = lower_body_previous_upper_body_pct_df.where(lower_body_above_previous_upper_body_boolean_df.values)
            negative_body_gap_df = upper_body_previous_lower_body_pct_df.where(previous_lower_body_above_upper_body_boolean_df.values)
            body_gap_pct_df = positive_body_gap_df.fillna(negative_body_gap_df).rename(columns={RuntimeIndicator.COMPARE: CustomisedIndicator.BODY_GAP})

            upper_body_df = upper_body_df.rename(columns={RuntimeIndicator.COMPARE: CustomisedIndicator.CANDLE_UPPER_BODY})
            lower_body_df = lower_body_df.rename(columns={RuntimeIndicator.COMPARE: CustomisedIndicator.CANDLE_LOWER_BODY})

            customised_indicator_df_list = [historical_data_df, 
                                            close_change_df, high_change_df, vol_change_df,
                                            upper_body_df, lower_body_df,
                                            colour_df,
                                            gap_pct_df, body_gap_pct_df,
                                            body_ratio_df, upper_shadow_ratio_df, lower_shadow_ratio_df,
                                            sma_50_volume_df]

            result_df = pd.concat(customised_indicator_df_list, axis=1)
            
            export_dir = f'{export_folder_dir}/full_historical_data_chunk_{index + 1}.csv'
            result_df.to_csv(export_dir)
            customised_historical_data_df_list.append(result_df)
            logger.debug(f'Full Historical Data Chunk {index + 1} Customised Indicator Appendage Time, {time.time() - append_start_time} seconds')

        logger.debug('Total Customised Indicator Appendage and Export Time, %s seconds' % (time.time() - start_time))
    except Exception as e:
        raise e
    
    return customised_historical_data_df_list