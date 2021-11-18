import time
import os
import logging

from config.config import config, ConfigProperty

from factory.datasource_factory import DataSourceFactory

from utils.common_util import log_msg
from utils.file_util import clean_txt_file_content

def init():
    start_time = time.time()
    
    try:
        log_dir = 'log.txt'
        clean_txt_file_content(log_dir)

        historical_data_source_type = config[ConfigProperty.HISTORICAL_DATA_SOURCE_TYPE]
        datasource = DataSourceFactory.get_datasource(historical_data_source_type)
        datasource.initialise_historical_data()

        log_msg(f'--- Total Stock Historical Data Initialisation Time, {(time.time() - start_time)} seconds ---')
    except Exception as e:
        print('Historical Data Initialisation Failed')
        logging.exception('Historical Data Initialisation Failed, Cause:')

init()