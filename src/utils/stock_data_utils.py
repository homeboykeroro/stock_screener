import time
import datetime
import os
import logging
import pandas as pd
import numpy as np

from constant.customised_indicator import CustomisedIndicator
from constant.indicator import Indicator
from config.config import config

from utils.common_util import log_msg

root_folder_dir = config['STOCK_DATA_ROOT_FOLDER_DIR']
idx = pd.IndexSlice

def get_stock_chart(ticker_list: list, filter_condition_dict: list) -> None:
    print('Number of Stock Charts: %s' % len(ticker_list))
    url_list = []

    for ticker in ticker_list:
        url = f'https://finviz.com/chart.ashx?t={ticker}&ty=c&ta=1&p=d'
        url_list.append(url)

    try:
        current_date = datetime.datetime.now().strftime('%Y%m%d')
        current_time = datetime.datetime.now().strftime('%H%M%S')
        export_file_dir = os.path.join(root_folder_dir, f'Charts/{current_date}{current_time}_{filter_condition_dict["pattern"]}/chart.html')
        os.mkdir(export_file_dir)

        header_html_str = f'<div style="font-size:23px; font-weight: bold; margin-bottom: 50px;">No. of Filtered Result: {str(len(ticker_list))}</div>'
        
        with open(export_file_dir, 'w') as chart_file:
            chart_file.write(header_html_str)

            for url in url_list:
                chart_file.write(f'<img style="width:900px; height:300px" src=\"{url}\"/>\n')

            chart_file.write(f'<div>{filter_condition_dict}</div>')
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
        print(f'Historical Data Loading Time, {(time.time() - start_time)} seconds')
    except Exception as e:
        raise e
    
    return df_list

def append_custom_indicators(folder_dir_list: list, export_folder_dir: str) -> None:
    start_time = time.time()
    historical_data_df_list = load_historical_data_into_df(folder_dir_list)
    
    try:
        for index, historical_data_df in enumerate(historical_data_df_list):
            append_start_time = time.time()
            
            #Close Change
            close_change_df = historical_data_df.loc[:, idx[:, Indicator.CLOSE]].pct_change().mul(100).rename(columns={Indicator.CLOSE: CustomisedIndicator.CLOSE_CHANGE})
            result_df = pd.concat([historical_data_df, close_change_df], axis=1)

            #High Change
            high_change_df = historical_data_df.loc[:, idx[:, Indicator.HIGH]].pct_change().mul(100).rename(columns={Indicator.HIGH: CustomisedIndicator.HIGH_CHANGE})
            result_df = pd.concat([result_df, high_change_df], axis=1)

            #Candle Upper/ Lower Body
            open_df = historical_data_df.loc[:, idx[:, Indicator.OPEN]].rename(columns={Indicator.OPEN: 'Compare'})
            close_df = historical_data_df.loc[:, idx[:, Indicator.CLOSE]].rename(columns={Indicator.CLOSE: 'Compare'})

            close_above_open_boolean_df = (close_df > open_df)
            close_above_open_df = close_df.where(close_above_open_boolean_df.values)
            open_above_close_df = open_df.where((~close_above_open_boolean_df).values)
            upper_body_df = close_above_open_df.fillna(open_above_close_df)
            result_df = pd.concat([result_df, upper_body_df.rename(columns={'Compare': CustomisedIndicator.CANDLE_UPPER_BODY})], axis=1)

            open_below_close_df = open_df.where(close_above_open_boolean_df.values)
            close_below_open_df = close_df.where((~close_above_open_boolean_df).values)
            lower_body_df = open_below_close_df.fillna(close_below_open_df)
            result_df = pd.concat([result_df, lower_body_df.rename(columns={'Compare': CustomisedIndicator.CANDLE_LOWER_BODY})], axis=1)

            #Normal Gap Up
            previois_close_df = close_df.shift()
            gap_up_diff_pct_df = ((open_df.sub(previois_close_df.values).replace(0, np.nan)).div(previois_close_df.values)).mul(100).rename(columns={'Compare': CustomisedIndicator.NORMAL_GAP})
            result_df = pd.concat([result_df, gap_up_diff_pct_df], axis=1)

            #Candle Body Gap Up
            previous_upper_body_df = upper_body_df.shift()
            body_gap_up_diff_pct_df = ((lower_body_df.sub(previous_upper_body_df.values).replace(0, np.nan)).div(previous_upper_body_df.values)).mul(100).rename(columns={'Compare': CustomisedIndicator.CANDLE_BODY_GAP})
            result_df = pd.concat([result_df, body_gap_up_diff_pct_df], axis=1)

            #Volume Change
            vol_change_df = historical_data_df.loc[:, idx[:, Indicator.VOLUME]].pct_change().mul(100).rename(columns={Indicator.VOLUME: CustomisedIndicator.VOLUME_CHANGE})
            result_df = pd.concat([result_df, vol_change_df], axis=1)

            #Volume Moving Average
            sma_50_volume_df = historical_data_df.loc[:, idx[:, Indicator.VOLUME]].rolling(window=50).mean().rename(columns={Indicator.VOLUME: CustomisedIndicator.MA_50_VOLUME})
            result_df = pd.concat([result_df, sma_50_volume_df], axis=1)

            log_msg(f'Chunk {index + 1} Append Custom Statistics Time, {time.time() - append_start_time} seconds')
            export_dir = f'{export_folder_dir}/full_historical_data_chunk_{index + 1}.csv'
            result_df.to_csv(export_dir)

        log_msg('Append and Export Time, %s seconds' % (time.time() - start_time))
    except Exception as e:
        logging.exception('Append and Export Failed, Cause: %s' % e)
        raise Exception('Append and Export Error')