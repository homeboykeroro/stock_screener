import time
from utils.stock_data_utils import *

def init():
    start_time = time.time()

    root_dir = config[ 'STOCK_DATA_ROOT_FOLDER_DIR' ]
    historical_data_source_type = config[ 'HISTORICAL_DATA_SOURCE_TYPE' ]

    clean_log_and_statistics()
    initialise_historical_data_directory()
    download_ticker_list_from_ftp()

    if DataSourceType.YFINANCE.name == historical_data_source_type:
        download_historical_data_from_yfinance( exchange='NQ' )
        download_historical_data_from_yfinance( exchange='OTHER' )

        append_custom_statistics_file_list = [ os.path.join( root_dir, 'Historical/Yfinance/Nasdaq' ), 
                                                os.path.join( root_dir, 'Historical/Yfinance/Others' )  ]
    elif DataSourceType.STOOQ.name == historical_data_source_type:
        extract_historical_data_from_stooq_files()

        append_custom_statistics_file_list = [ os.path.join( root_dir, 'Historical/Stooq/Nasdaq' ), 
                                                os.path.join( root_dir, 'Historical/Stooq/Nyse' ), 
                                                os.path.join( root_dir, 'Historical/Stooq/Amex' ) ]
    
    append_custom_indicators( append_custom_statistics_file_list )

    log_msg( "--- Total Stock Historical Data Initialisation Time, %s seconds ---" % ( time.time() - start_time ) )

init()