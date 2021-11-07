from enum import Enum

import json
import logging
import os

class ConfigProperty(str, Enum):
    STOCK_DATA_ROOT_FOLDER_DIR = 'STOCK_DATA_ROOT_FOLDER_DIR',
    HISTORICAL_DATA_SOURCE_TYPE = 'HISTORICAL_DATA_SOURCE_TYPE'
    STOCK_HISTORICAL_DATA_START_DATE = 'STOCK_HISTORICAL_DATA_START_DATE',
    HISTORICAL_DATA_DOWNLOAD_CHUNK_SIZE = 'HISTORICAL_DATA_DOWNLOAD_CHUNK_SIZE',

def init_config():
    try:
        with open('config.json', 'r') as config_json:
            config = json.load(config_json)
        
        LOG_FILE_PATH = os.path.join(config[ConfigProperty.STOCK_DATA_ROOT_FOLDER_DIR], 'log.txt')
        logging.basicConfig(filename=LOG_FILE_PATH, format='\r%(asctime)s - %(message)s (%(levelname)s)', 
                        datefmt='%m/%d/%Y %I:%M:%S %p', level=logging.INFO)
    except Exception as e:
        logging.exception('Read Config File Failed, Cause: %s' % e)
        raise Exception('Read Config File Error')
    
    return config

config = init_config()
