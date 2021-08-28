import time
from stock_data_utils import *

def init():
    start_time = time.time()

    root_dir = config[ 'STOCK_DATA_ROOT_FOLDER_DIR' ]
    nasdaq_historical_data_folder_dir = os.path.join( root_dir, 'Historical/Nasdaq/' )
    other_historical_data_folder_dir = os.path.join( root_dir, 'Historical/Others/' )
    full_historical_data_folder_dir = os.path.join( root_dir, 'Historical/Customised/' )
    result_data_folder_dir = os.path.join( root_dir, 'Charts/' )
    clean_folder_dir_list = [ nasdaq_historical_data_folder_dir, other_historical_data_folder_dir, full_historical_data_folder_dir, result_data_folder_dir ]

    clean_folder_contents( clean_folder_dir_list )
    download_ticker_list_from_ftp()
    download_stock_historical_data( exchange='NQ' )
    download_stock_historical_data( exchange='OTHER' )
    append_custom_statistics()

    print( "--- Total Stock Historical Data Initialisation Time, %s seconds ---" % ( time.time() - start_time ) )

init()