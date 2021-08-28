import json
import logging
import os

def init_config():
    config = {}

    try:
        with open( 'config.json', 'r' ) as config_json:
            json_config = json.load( config_json )

        config[ 'STOCK_HISTORICAL_DATA_START_DATE' ] = json_config[ 'STOCK_HISTORICAL_DATA_START_DATE' ]
        config[ 'HISTORICAL_DATA_DOWNLOAD_CHUNK_SIZE' ] = json_config[ 'HISTORICAL_DATA_DOWNLOAD_CHUNK_SIZE' ]
        config[ 'STOCK_DATA_ROOT_FOLDER_DIR' ] = json_config[ 'STOCK_DATA_ROOT_FOLDER_DIR' ]
        
        LOG_FILE_PATH = os.path.join( config[ 'STOCK_DATA_ROOT_FOLDER_DIR' ], 'log.txt' )
        logging.basicConfig( filename=LOG_FILE_PATH, format='\r%(asctime)s - %(message)s (%(levelname)s)', 
                        datefmt='%m/%d/%Y %I:%M:%S %p', level=logging.INFO )
    except Exception as e:
        logging.exception( 'Read Config File Failed, Cause: %s' % e )
        raise Exception( 'Read Config File Error' )
    
    return config

config = init_config()
