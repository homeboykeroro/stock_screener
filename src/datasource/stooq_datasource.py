import time
import datetime
import glob
import os
from pathlib import Path

import pandas as pd

from config.config import config, ConfigProperty
from datasource.datasource import DataSource
from constant.stock_exchange import StockExchange
from constant.indicator import Indicator

from utils.common_util import *
from utils.file_util import create_dir, clean_dir
from utils.stock_data_utils import load_historical_data_into_df, append_custom_indicators

class StooqDataSource(DataSource):
    def __init__(self) -> None:
        super().__init__()
        chunk_size = config[ConfigProperty.HISTORICAL_DATA_DOWNLOAD_CHUNK_SIZE]
        start_date = config[ConfigProperty.STOCK_HISTORICAL_DATA_START_DATE]
        root_folder_dir = config[ConfigProperty.STOCK_DATA_ROOT_FOLDER_DIR]
        stooq_data_folder_dir = os.path.join(config[ConfigProperty.STOCK_DATA_ROOT_FOLDER_DIR], f'Historical/Stooq/')
        
        self.__chunk_size = chunk_size
        self.__start_date = start_date
        self.__src_folder_dir = root_folder_dir
        self.__total_no_of_data_file = 0
        self.__stooq_data_folder_dir = stooq_data_folder_dir
        self.__src_zip_dir = os.path.join(root_folder_dir, 'd_us_txt.zip')
        self.__chart_dir = os.path.join(root_folder_dir, 'Chart')
        self.__complete_stock_data_dir = os.path.join(stooq_data_folder_dir, 'FULL')
        self.__nasdaq_stock_data_dir = os.path.join(stooq_data_folder_dir, StockExchange.NASDAQ)
        self.__nyse_stock_data_dir = os.path.join(stooq_data_folder_dir, StockExchange.NYSE)
        self.__amex_stock_data_dir = os.path.join(stooq_data_folder_dir, StockExchange.AMEX)

    def initialise_historical_data(self) -> None:
        self.__initialise_src_folder_dir()
        self.__extract_data_from_stooq_file_and_export()
        append_custom_indicators([self.__nasdaq_stock_data_dir, 
                                self.__nyse_stock_data_dir, 
                                self.__amex_stock_data_dir], self.__complete_stock_data_dir)

    def get_historical_data_df_list(self):
        return load_historical_data_into_df([self.__complete_stock_data_dir])

    def __initialise_src_folder_dir(self):
        clean_folder_dir_list = [self.__nasdaq_stock_data_dir, 
                                self.__nyse_stock_data_dir, 
                                self.__amex_stock_data_dir,
                                self.__complete_stock_data_dir,
                                self.__chart_dir]
        
        if os.path.exists(self.__src_zip_dir):
            clean_dir(self.__src_zip_dir)

        for dir in clean_folder_dir_list:
            if not os.path.exists(dir):
                create_dir(dir)
            else:
                clean_dir(dir)

    def __extract_data_from_stooq_file_and_export(self):
        print('Start Extracting Stooq Historical Data...')

        stock_exchange_to_historical_data_file_dir_pattern_dict = {
            StockExchange.NASDAQ: 'data/**/nasdaq stocks/**/*.txt', 
            StockExchange.NYSE: 'data/**/nyse stocks/**/*.txt', 
            StockExchange.AMEX: 'data/**/nysemkt stocks/**/*.txt' 
       }

        for stock_exchange, file_pattern in stock_exchange_to_historical_data_file_dir_pattern_dict.items():
            start_time = time.time()
            self.__construct_historical_data_df_and_write(stock_exchange, file_pattern)
            log_msg(f'{stock_exchange} Stock Historical Data Extract Time, {(time.time() - start_time)} seconds')

        log_msg(f'Total No. of Ticker File: {self.__total_no_of_data_file}')

    def __construct_historical_data_df_and_write(self, stock_exchange: str, file_pattern: str):
        idx = pd.IndexSlice

        historical_data_file_dir_list = glob.glob(os.path.join(self.__src_folder_dir, file_pattern), recursive=True)
        historical_data_file_dir_list = [file_dir for file_dir in historical_data_file_dir_list if len(Path(file_dir).stem.split('.')[0]) <= 4 and Path(file_dir).stem.split('.')[0].isalpha() and os.stat(file_dir).st_size > 0]
        file_dir_chunk_list = [historical_data_file_dir_list[x: x + self.__chunk_size] for x in range(0, len(historical_data_file_dir_list), self.__chunk_size)]
        
        log_msg(f'No. of {stock_exchange} Ticker File: {len(historical_data_file_dir_list)}')
        self.__total_no_of_data_file += len(historical_data_file_dir_list)

        indicator_list = [Indicator.OPEN, Indicator.HIGH, Indicator.LOW, Indicator.CLOSE, Indicator.VOLUME]
        stooq_historical_data_column_list = ['<TICKER>', '<DATE>', '<OPEN>', '<HIGH>', '<LOW>', '<CLOSE>', '<VOL>']
        renamed_src_historical_data_column_list = ['Ticker', 'Date'] + indicator_list

        for chunk_no, file_dir_chunk in enumerate(file_dir_chunk_list):
            chunk_export_start_time = time.time()

            for file_idx, stock_historical_file in enumerate(file_dir_chunk):
                stock_historical_data_df = pd.read_csv(stock_historical_file, usecols=stooq_historical_data_column_list)
                ticker_symbol = stock_historical_data_df.iloc[0].values[0].split('.')[0]

                stock_historical_data_df.index = pd.to_datetime(stock_historical_data_df.loc[:, '<DATE>'], format='%Y%m%d')
                stock_historical_data_df.index.name = 'Date'
                stock_historical_data_df.columns = pd.MultiIndex.from_product([[ticker_symbol], renamed_src_historical_data_column_list]) 
                stock_historical_data_df = stock_historical_data_df.loc[self.__start_date: datetime.datetime.now().strftime('%Y-%m-%d'), idx[:, indicator_list]]

                if file_idx == 0:
                    full_stock_historical_data_df = stock_historical_data_df
                else:
                    full_stock_historical_data_df = pd.concat([full_stock_historical_data_df, stock_historical_data_df], axis=1)

            historical_data_chunk_file_dir = os.path.join(self.__stooq_data_folder_dir, f'{stock_exchange}/historical_data_chunk_{str(chunk_no + 1)}.csv')
            full_stock_historical_data_df.to_csv(historical_data_chunk_file_dir)
            log_msg(f'{stock_exchange} Stock Historical Data Chunk {(chunk_no + 1)} Size: {len(file_dir_chunk)}, Export Time, {(time.time() - chunk_export_start_time)} seconds')