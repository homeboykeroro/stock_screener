import time
import datetime
import glob
import os
from pathlib import Path

import pandas as pd

from config.config import config, ConfigProperty
from datasource.datasource import DataSource
from constant.stock_exchange import StockExchange
from constant.indicator.indicator import Indicator

from utils.log_util import get_logger
from utils.file_util import create_dir, clean_dir, clean_txt_file_content
from utils.stock_data_utils import load_historical_data_into_df, append_custom_indicators

root_logger = get_logger()
stooq_logger = get_logger(name='stooq_datasource', 
                        log_dir=os.path.join(config[ConfigProperty.STOCK_DATA_ROOT_FOLDER_DIR], 'Historical/stooq_data_statistics.txt'),
                        display_format='%(message)s')

class StooqDataSource(DataSource):
    def __init__(self) -> None:
        super().__init__()
        chunk_size = config[ConfigProperty.HISTORICAL_DATA_DOWNLOAD_CHUNK_SIZE]
        start_date = config[ConfigProperty.STOCK_HISTORICAL_DATA_START_DATE]
        root_folder_dir = config[ConfigProperty.STOCK_DATA_ROOT_FOLDER_DIR]
        stooq_data_folder_dir = os.path.join(root_folder_dir, f'Historical/Stooq/')
        
        self.__chunk_size = chunk_size
        self.__start_date = start_date
        self.__src_folder_dir = root_folder_dir
        self.__total_no_of_data_file = 0
        self.__total_no_of_valid_data_file = 0
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
        customised_historical_data_df_list = append_custom_indicators([self.__nasdaq_stock_data_dir, 
                                                                        self.__nyse_stock_data_dir,
                                                                        self.__amex_stock_data_dir], self.__complete_stock_data_dir)
        
        stooq_logger.debug(f"\r{'-'*60}")

        for index, customised_historical_data_df in enumerate(customised_historical_data_df_list):
            stooq_logger.debug(f'\rFull Historical Data Chunk {index + 1}, No. of Row: {customised_historical_data_df.shape[0]}, No. of Column: {customised_historical_data_df.shape[1]}')
            stooq_logger.debug(f'\rNo. of Ticker: {len(customised_historical_data_df.columns.get_level_values(0).unique().tolist())}')
            stooq_logger.debug(f'No. of Indicator: {len(customised_historical_data_df.columns.get_level_values(1).unique().tolist())}')
            stooq_logger.debug(f"\r{'-'*60}")

    def get_historical_data_df_list(self):
        return load_historical_data_into_df([self.__complete_stock_data_dir])

    def __initialise_src_folder_dir(self):
        clean_folder_dir_list = [self.__nasdaq_stock_data_dir, 
                                self.__nyse_stock_data_dir, 
                                self.__amex_stock_data_dir,
                                self.__complete_stock_data_dir,
                                self.__chart_dir]
        
        #Clean Stooq Data Statistics
        clean_txt_file_content(os.path.join(config[ConfigProperty.STOCK_DATA_ROOT_FOLDER_DIR], 'Historical/stooq_data_statistics.txt'))

        #Delete Zip
        if os.path.exists(self.__src_zip_dir):
            clean_dir(self.__src_zip_dir)

        #Create Folder If Not Exists Otherwise Delete All Files/Sub Folder in That Folder
        for dir in clean_folder_dir_list:
            if not os.path.exists(dir):
                create_dir(dir)
                root_logger.debug(f'Create Directory: {dir}')
            else:
                clean_dir(dir)
                root_logger.debug(f'Clean Directory: {dir}')

    def __extract_data_from_stooq_file_and_export(self):
        root_logger.debug('Start Extracting Stooq Historical Data...')

        stock_exchange_to_historical_data_file_dir_pattern_dict = {
            StockExchange.NASDAQ: 'data/**/nasdaq stocks/**/*.txt', 
            StockExchange.NYSE: 'data/**/nyse stocks/**/*.txt', 
            StockExchange.AMEX: 'data/**/nysemkt stocks/**/*.txt' 
       }

        for stock_exchange, file_pattern in stock_exchange_to_historical_data_file_dir_pattern_dict.items():
            start_time = time.time()
            self.__construct_historical_data_df_and_write(stock_exchange, file_pattern)
            root_logger.debug(f'{stock_exchange} Stock Historical Data Extract Time, {(time.time() - start_time)} seconds')

        stooq_logger.debug(f"\r{'-'*60}")
        stooq_logger.debug(f'\rTotal No. of Stooq Ticker File: {self.__total_no_of_data_file}')
        stooq_logger.debug(f'Total No. of Valid Common Stock File: {self.__total_no_of_valid_data_file}')

    def __construct_historical_data_df_and_write(self, stock_exchange: str, file_pattern: str):
        idx = pd.IndexSlice

        historical_data_file_dir_list = glob.glob(os.path.join(self.__src_folder_dir, file_pattern), recursive=True)
        valid_historical_data_file_dir_list = [file_dir for file_dir in historical_data_file_dir_list if len(Path(file_dir).stem.split('.')[0]) <= 4 and Path(file_dir).stem.split('.')[0].isalpha() and os.stat(file_dir).st_size > 0]
        file_dir_chunk_list = [valid_historical_data_file_dir_list[x: x + self.__chunk_size] for x in range(0, len(valid_historical_data_file_dir_list), self.__chunk_size)]
        
        stooq_logger.debug(f"\r{'-'*60}")
        stooq_logger.debug(f'\rNo. of Stooq {stock_exchange} Ticker File: {len(historical_data_file_dir_list)}')
        stooq_logger.debug(f'No. of Valid {stock_exchange} Common Stock File: {len(valid_historical_data_file_dir_list)}\r')
        self.__total_no_of_data_file += len(historical_data_file_dir_list)
        self.__total_no_of_valid_data_file += len(valid_historical_data_file_dir_list)

        indicator_list = [Indicator.OPEN, Indicator.HIGH, Indicator.LOW, Indicator.CLOSE, Indicator.VOLUME]
        stooq_historical_data_column_list = ['<OPEN>', '<HIGH>', '<LOW>', '<CLOSE>', '<VOL>']
        ticker_column_list = ['<TICKER>']
        date_column_list = ['<DATE>']

        for chunk_no, file_dir_chunk in enumerate(file_dir_chunk_list):
            chunk_export_start_time = time.time()
            data_df_list = []

            for stock_historical_file in file_dir_chunk:
                date_df = pd.read_csv(stock_historical_file, usecols=date_column_list)
                date_index = pd.to_datetime(date_df.values.flatten(), format='%Y%m%d')
                
                ticker_df = pd.read_csv(stock_historical_file, usecols=ticker_column_list)
                ticker_symbol = ticker_df.iloc[0].values[0].split('.')[0]
                indicator_column = pd.MultiIndex.from_product([[ticker_symbol], indicator_list])

                indicator_data_df = pd.read_csv(stock_historical_file, usecols=stooq_historical_data_column_list)
                stock_historical_data_df = pd.DataFrame(indicator_data_df.values, index=date_index, columns=indicator_column)
                stock_historical_data_df = stock_historical_data_df.loc[self.__start_date: datetime.datetime.now().strftime('%Y-%m-%d')]
                data_df_list.append(stock_historical_data_df)

            historical_data_chunk_file_dir = os.path.join(self.__stooq_data_folder_dir, f'{stock_exchange}/historical_data_chunk_{str(chunk_no + 1)}.csv')
            full_stock_historical_data_df = pd.concat(data_df_list, axis=1)
            full_stock_historical_data_df.to_csv(historical_data_chunk_file_dir)
            
            root_logger.debug(f'{stock_exchange} Stock Historical Data Chunk {(chunk_no + 1)}, Export Time, {(time.time() - chunk_export_start_time)} seconds')
            stooq_logger.debug(f'{stock_exchange} Stock Historical Data Chunk {(chunk_no + 1)} Size: {len(data_df_list)}')