import time
import os
import logging

from config.config import config, ConfigProperty

from factory.datasource_factory import DataSourceFactory

from utils.common_util import log_msg
from utils.file_util import clean_file_content

def init():
    start_time = time.time()
    
    try:
        root_dir = config[ConfigProperty.STOCK_DATA_ROOT_FOLDER_DIR]
        log_dir = os.path.join(root_dir, 'log.txt')
        clean_file_content(log_dir)

        historical_data_source_type = config[ConfigProperty.HISTORICAL_DATA_SOURCE_TYPE]
        datasource = DataSourceFactory.get_datasource(historical_data_source_type)
        datasource.initialise_historical_data()

        log_msg(f'--- Total Stock Historical Data Initialisation Time, {(time.time() - start_time)} seconds ---')
    except Exception as e:
        logging.exception('Historical Data Initialisation Failed, Cause: {e}')

init()