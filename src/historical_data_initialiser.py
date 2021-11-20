import time

from config.config import config, ConfigProperty

from factory.datasource_factory import DataSourceFactory

from utils.log_util import get_logger
from utils.file_util import clean_txt_file_content

logger = get_logger()

def init():
    start_time = time.time()
    
    try:
        log_dir = 'log.txt'
        clean_txt_file_content(log_dir)

        historical_data_source_type = config[ConfigProperty.HISTORICAL_DATA_SOURCE_TYPE]
        datasource = DataSourceFactory.get_datasource(historical_data_source_type)
        datasource.initialise_historical_data()

        logger.debug(f'--- Total Stock Historical Data Initialisation Time, {(time.time() - start_time)} seconds ---')
    except Exception as e:
        logger.exception('Historical Data Initialisation Failed, Cause:')

init()