import json
import logging
import os

def init_config():
    try:
        with open( 'config.json', 'r' ) as config_json:
            config = json.load( config_json )
        
        LOG_FILE_PATH = os.path.join( config[ 'STOCK_DATA_ROOT_FOLDER_DIR' ], 'log.txt' )
        logging.basicConfig( filename=LOG_FILE_PATH, format='\r%(asctime)s - %(message)s (%(levelname)s)', 
                        datefmt='%m/%d/%Y %I:%M:%S %p', level=logging.INFO )
    except Exception as e:
        logging.exception( 'Read Config File Failed, Cause: %s' % e )
        raise Exception( 'Read Config File Error' )
    
    return config

config = init_config()
