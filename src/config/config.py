from enum import Enum

import json
import logging
import os

from utils.file_util import create_dir

class ConfigProperty(str, Enum):
    STOCK_DATA_ROOT_FOLDER_DIR = 'STOCK_DATA_ROOT_FOLDER_DIR',
    HISTORICAL_DATA_SOURCE_TYPE = 'HISTORICAL_DATA_SOURCE_TYPE',
    STOCK_HISTORICAL_DATA_START_DATE = 'STOCK_HISTORICAL_DATA_START_DATE',
    HISTORICAL_DATA_DOWNLOAD_CHUNK_SIZE = 'HISTORICAL_DATA_DOWNLOAD_CHUNK_SIZE'

def init_config():
    try:
        with open('config.json', 'r') as config_json:
            config = json.load(config_json)
        
        log_dir = 'log.txt'

        if not os.path.exists(log_dir):
            create_dir(log_dir, log_msg=False)

        logging.basicConfig(filename=log_dir, format='\r%(asctime)s - %(message)s (%(levelname)s)', 
                        datefmt='%m/%d/%Y %I:%M:%S %p', level=logging.INFO)
    except Exception as e:
        logging.exception('Read Config File Failed, Cause: %s' % e)
        raise Exception('Read Config File Error')
    
    return config

config = init_config()
