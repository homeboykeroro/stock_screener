from enum import Enum
import json

from utils.log_util import get_logger

class ConfigProperty(str, Enum):
    STOCK_DATA_ROOT_FOLDER_DIR = 'STOCK_DATA_ROOT_FOLDER_DIR',
    HISTORICAL_DATA_SOURCE_TYPE = 'HISTORICAL_DATA_SOURCE_TYPE',
    STOCK_HISTORICAL_DATA_START_DATE = 'STOCK_HISTORICAL_DATA_START_DATE',
    HISTORICAL_DATA_DOWNLOAD_CHUNK_SIZE = 'HISTORICAL_DATA_DOWNLOAD_CHUNK_SIZE'

logger = get_logger()

def init_config():
    try:
        with open('config.json', 'r') as config_json:
            config = json.load(config_json)
    except Exception as e:
        logger.exception('Read Config File Failed, Cause: %s' % e)
        raise Exception('Read Config File Error')
    
    return config

config = init_config()
