import time
import glob
from pathlib import Path
import datetime
import os, shutil
import logging
import ftplib
import pandas as pd
import yfinance as yf
from config.config import config

from utils.common_utils import *
from config.data_source_type import DataSourceType
idx = pd.IndexSlice

root_dir = config[ 'STOCK_DATA_ROOT_FOLDER_DIR' ]
historical_data_source_type = config[ 'HISTORICAL_DATA_SOURCE_TYPE' ]
chunk_size = config[ 'HISTORICAL_DATA_DOWNLOAD_CHUNK_SIZE' ]
start_date = config[ 'STOCK_HISTORICAL_DATA_START_DATE' ]

def initialise_historical_data_directory():
    start_time = time.time()

    if DataSourceType.YFINANCE.name == historical_data_source_type:
        folder_dir_list = [ os.path.join( root_dir, 'Historical/Yfinance/Nasdaq' ), 
                            os.path.join( root_dir, 'Historical/Yfinance/Others' ), 
                            os.path.join( root_dir, 'Historical/Yfinance/Full' ), 
                            os.path.join( root_dir, 'Charts' ) ]
    elif DataSourceType.STOOQ.name == historical_data_source_type:
        folder_dir_list = [ os.path.join( root_dir, 'Historical/Stooq/Nasdaq' ), 
                            os.path.join( root_dir, 'Historical/Stooq/Nyse' ), 
                            os.path.join( root_dir, 'Historical/Stooq/Amex' ), 
                            os.path.join( root_dir, 'Historical/Stooq/Full' ), 
                            os.path.join( root_dir, 'Charts' ) ]

    try:
        for stock_src_folder_content in os.listdir( root_dir ):
            if stock_src_folder_content == 'log.txt' or stock_src_folder_content == 'statistics.txt' or stock_src_folder_content == 'data':
                continue

            full_dir = root_dir + stock_src_folder_content
            if os.path.isfile( full_dir ):
                os.unlink( full_dir )
                log_msg( 'Delete Directory: %s' % full_dir )

        for folder_dir in folder_dir_list:
            if not os.path.exists( folder_dir ):
                os.makedirs( folder_dir )
                log_msg( 'Create Directory: %s' % folder_dir )
            else:
                for content_dir in os.listdir( folder_dir ):
                    full_dir = folder_dir + '/' + content_dir
                    if os.path.isfile( full_dir ):
                        os.unlink( full_dir )
                    elif os.path.isdir( full_dir ):
                        shutil.rmtree( full_dir )
                    log_msg( 'Delete Directory: %s' % full_dir )

        print( "--- Clean Stock Data Source Folder Time, %s seconds ---" % ( time.time() - start_time ) )
    except Exception as e:
        logging.exception( 'Clean Stock Data Source Folder Failed, Path: %s. Cause: %s' % ( full_dir, e ) )
        raise Exception( 'Clean Stock Data Source Folder Error' )

def clean_log_and_statistics():
    log_dir = os.path.join( root_dir, 'log.txt' )
    statistics_dir = os.path.join( root_dir, 'statistics.txt' )

    with open( log_dir, 'r+' ) as log:
        log.truncate( 0 )

    with open( statistics_dir, 'r+' ) as statistics:
        statistics.truncate( 0 )

def download_ticker_list_from_ftp():
    start_time = time.time()

    FTP_HOST = 'ftp.nasdaqtrader.com'
    FILENAME_LIST = [ 'otherlisted.txt', 'nasdaqlisted.txt' ]
    ftp = ftplib.FTP()

    try:
        ftp.connect( FTP_HOST )
        ftp.login()
        ftp.cwd( '/Symboldirectory' )
        print( ftp.dir() )

        for FILENAME in FILENAME_LIST:
            local_dir = os.path.join( root_dir, os.path.basename( FILENAME ) )
            with open( local_dir, "wb" ) as file:
                ftp.retrbinary( f'RETR {FILENAME}', file.write )
        
        print( 'Ticker List Download from FTP Completed' )
    except Exception as e:
        logging.exception( 'Ticker List File Download from FTP Failed, Cause: %s' %e )
        raise Exception( 'FTP Download Error' )
    finally:
        ftp.quit()
        print( "Close FTP Session" )
        print( "--- Ticker List Download From FTP Time, %s seconds ---" % ( time.time() - start_time ) )

def get_ticker_chunk_list( chunk_size: int ):
    nasdaq_tickers_dir = os.path.join( root_dir, 'nasdaqlisted.txt' )
    other_tickers_dir = os.path.join( root_dir, 'otherlisted.txt' )
    statistics_dir = os.path.join( root_dir, 'statistics.txt' )

    nasdaq_tickers_df = pd.read_csv( nasdaq_tickers_dir, sep='|' )
    other_tickers_df = pd.read_csv( other_tickers_dir, sep='|' )

    #Extract Common Stock Ticker
    nasdaq_filter_condition = ( nasdaq_tickers_df[ 'ETF' ] == 'N' ) & ( nasdaq_tickers_df[ 'Symbol' ].str.len() <= 4 ) & ( nasdaq_tickers_df[ 'Symbol' ].str.isalpha() )
    filtered_nasdaq_tickers_df = nasdaq_tickers_df.loc[ nasdaq_filter_condition, [ 'Symbol' ] ]
    other_tickers_condition = ( other_tickers_df[ 'ETF' ] == 'N' ) & ( other_tickers_df[ 'ACT Symbol' ].str.len() <= 4 ) & ( other_tickers_df[ 'ACT Symbol' ].str.isalpha() )
    filtered_other_tickers_df = other_tickers_df.loc[ other_tickers_condition, [ 'ACT Symbol' ] ].rename( columns = { 'ACT Symbol': 'Symbol' } )

    #Extract ETF Ticker  
    nasdaq_etf_filter_condition = ( nasdaq_tickers_df[ 'ETF' ] == 'Y' ) & ( nasdaq_tickers_df[ 'Symbol' ].str.len() <= 4 ) & ( nasdaq_tickers_df[ 'Symbol' ].str.isalpha() )
    filtered_nasdaq_etf_tickers_df = nasdaq_tickers_df.loc[ nasdaq_etf_filter_condition, [ 'Symbol' ] ]
    other_etf_tickers_condition = ( other_tickers_df[ 'ETF' ] == 'Y' ) & ( other_tickers_df[ 'ACT Symbol' ].str.len() <= 4 ) & ( other_tickers_df[ 'ACT Symbol' ].str.isalpha() )
    filtered_other_etf_tickers_df = other_tickers_df.loc[ other_etf_tickers_condition, [ 'ACT Symbol' ] ].rename( columns = { 'ACT Symbol': 'Symbol' } )

    #Record No. of Common Stock/ ETF
    with open( statistics_dir, 'a' ) as file:
        file.write( '--------------- TICKERS FROM NASDAQ TRADER FTP SOURCE ---------------\r' )
        file.write( 'No. of Listed Nasdaq Stock Tickers: %d \r' % len( filtered_nasdaq_tickers_df ) )
        file.write( 'No. of Listed Other Stock Tickers: %d \r' % len( filtered_other_tickers_df ) )
        file.write( 'No. of Listed Common Stock Tickers: %d \r' % ( len( filtered_nasdaq_tickers_df ) + len( filtered_other_tickers_df ) ) )

        file.write( '\rNo. of Listed Nasdaq ETF Tickers: %d \r' % len( filtered_nasdaq_etf_tickers_df ) )
        file.write( 'No. of Listed Other ETF Tickers: %d \r' % len( filtered_other_etf_tickers_df ) )
        file.write( 'No. of Listed ETF Tickers: %d' % ( len( filtered_nasdaq_etf_tickers_df ) + len( filtered_other_etf_tickers_df ) ) )

    nasdaq_ticker_list = filtered_nasdaq_tickers_df.loc[ :,'Symbol' ].values.tolist()
    other_tickers_list = filtered_other_tickers_df.loc[ :, 'Symbol' ].values.tolist()
    nasdaq_etf_ticker_list = filtered_nasdaq_etf_tickers_df.loc[ :,'Symbol' ].values.tolist()
    other_etf_tickers_list = filtered_other_etf_tickers_df.loc[ :, 'Symbol' ].values.tolist()

    #Put Tickers into Chunks
    nasdaq_ticker_chunks = [ nasdaq_ticker_list[ x : x + chunk_size ] for x in range( 0, len( nasdaq_ticker_list ), chunk_size ) ]
    other_ticker_chunks = [ other_tickers_list[ x : x + chunk_size ] for x in range( 0, len( other_tickers_list ), chunk_size ) ]

    nasdaq_etf_ticker_chunks = [ nasdaq_etf_ticker_list[ x : x + chunk_size ] for x in range( 0, len( nasdaq_etf_ticker_list ), chunk_size ) ]
    other_etf_ticker_chunks = [ other_etf_tickers_list[ x : x + chunk_size ] for x in range( 0, len( other_etf_tickers_list ), chunk_size ) ]

    ticker_info_map = {
        'nasdaq_ticker_chunks': nasdaq_ticker_chunks,
        'other_ticker_chunks': other_ticker_chunks,
        'nasdaq_etf_ticker_chunks': nasdaq_etf_ticker_chunks,
        'other_etf_ticker_chunks': other_etf_ticker_chunks,
        'nasdaq_ticker_list': nasdaq_ticker_list,
        'other_tickers_list': other_tickers_list,
        'nasdaq_etf_ticker_list': nasdaq_etf_ticker_list,
        'other_etf_tickers_list': other_etf_tickers_list
    }

    return ticker_info_map

def extract_historical_data_from_stooq_files():
    start_time = time.time()

    try:
        #Get Common Stock File List for All Exchange( NASDAQ, NYSE, AMEX )
        src_nasdaq_stock_historical_file_list = glob.glob( os.path.join( root_dir, 'data/**/nasdaq stocks/**/*.txt' ), recursive=True )
        src_nyse_stock_historical_file_list = glob.glob( os.path.join( root_dir, 'data/**/nyse stocks/**/*.txt' ), recursive=True )
        src_amex_stock_historical_file_list = glob.glob( os.path.join( root_dir, 'data/**/nysemkt stocks/**/*.txt' ), recursive=True )

        if len ( src_nasdaq_stock_historical_file_list ) == 0 and len( src_nyse_stock_historical_file_list ) == 0 and len( src_amex_stock_historical_file_list ) == 0:
            raise Exception( "Stooq Historical Data Files Not Found" )

        nasdaq_common_stock_historical_file_list = [ src_historical_file for src_historical_file in src_nasdaq_stock_historical_file_list if len( Path( src_historical_file ).stem.split( '.' )[ 0 ] ) <= 4 and Path( src_historical_file ).stem.split( '.' )[ 0 ].isalpha() and os.stat( src_historical_file ).st_size > 0 ]
        nyse_common_stock_historical_file_list = [ src_historical_file for src_historical_file in src_nyse_stock_historical_file_list if len( Path( src_historical_file ).stem.split( '.' )[ 0 ] ) <= 4 and Path( src_historical_file ).stem.split( '.' )[ 0 ].isalpha() and os.stat( src_historical_file ).st_size > 0 ]
        amex_common_stock_historical_file_list = [ src_historical_file for src_historical_file in src_amex_stock_historical_file_list if len( Path( src_historical_file ).stem.split( '.' )[ 0 ] ) <= 4 and Path( src_historical_file ).stem.split( '.' )[ 0 ].isalpha() and os.stat( src_historical_file ).st_size > 0 ]

        #Get ETF File List for All Exchange( NASDAQ, NYSE, AMEX )
        src_nasdaq_etf_historical_file_list = glob.glob( os.path.join( root_dir, 'data/**/nasdaq etfs/**/*.txt' ), recursive=True )
        src_nyse_etf_historical_file_list = glob.glob( os.path.join( root_dir, 'data/**/nyse etfs/**/*.txt' ), recursive=True )
        src_amex_etf_historical_file_list = glob.glob( os.path.join( root_dir, 'data/**/nysemkt etfs/**/*.txt' ), recursive=True )

        nasdaq_etf_historical_file_list = [ src_historical_file for src_historical_file in src_nasdaq_etf_historical_file_list if len( Path( src_historical_file ).stem.split( '.' )[ 0 ] ) <= 4 and Path( src_historical_file ).stem.split( '.' )[ 0 ].isalpha() and os.stat( src_historical_file ).st_size > 0 ]
        nyse_etf_historical_file_list = [ src_historical_file for src_historical_file in src_nyse_etf_historical_file_list if len( Path( src_historical_file ).stem.split( '.' )[ 0 ] ) <= 4 and Path( src_historical_file ).stem.split( '.' )[ 0 ].isalpha() and os.stat( src_historical_file ).st_size > 0 ]
        amex_etf_historical_file_list = [ src_historical_file for src_historical_file in src_amex_etf_historical_file_list if len( Path( src_historical_file ).stem.split( '.' )[ 0 ] ) <= 4 and Path( src_historical_file ).stem.split( '.' )[ 0 ].isalpha() and os.stat( src_historical_file ).st_size > 0 ]

        ticker_info_map = get_ticker_chunk_list( chunk_size )

        #Record No. of Common Stock/ ETF
        ticker_remark_dir = os.path.join( root_dir, 'statistics.txt' )

        with open( ticker_remark_dir, 'a' ) as file:
            file.write( '\r\r\r---------------------- TICKERS FROM STOOQ DATA ----------------------\r' )
            file.write( 'No. of Nasdaq Common Stock Historical Data Files: %d \r' % len( nasdaq_common_stock_historical_file_list ) )
            file.write( 'No. of Nyse Common Stock Historical Data Files: %d \r' % len( nyse_common_stock_historical_file_list ) )
            file.write( 'No. of Amex Common Stock Historical Data Files: %d \r' % len( amex_common_stock_historical_file_list ) )
            file.write( 'No. of Common Stock Historical Data Files: %d \r' % ( len( nasdaq_common_stock_historical_file_list ) + len( nyse_common_stock_historical_file_list ) + len( amex_common_stock_historical_file_list ) ) )

            file.write( '\rNo. of Nasdaq ETF Historical Data Files: %d \r' % len( nasdaq_etf_historical_file_list ) )
            file.write( 'No. of Nyse ETF Historical Data Files: %d \r' % len( nyse_etf_historical_file_list ) )
            file.write( 'No. of Amex ETF Historical Data Files: %d \r' % len( amex_etf_historical_file_list ) )
            file.write( 'No. of ETF Historical Data Files: %d \r' % ( len( nasdaq_etf_historical_file_list ) + len( nyse_etf_historical_file_list ) + len( amex_etf_historical_file_list ) ) )

        nasdaq_common_stock_historical_file_chunk_list = [ nasdaq_common_stock_historical_file_list[ x : x + chunk_size ] for x in range( 0, len( nasdaq_common_stock_historical_file_list ), chunk_size ) ]
        nyse_common_stock_historical_file_chunk_list = [ nyse_common_stock_historical_file_list[ x : x + chunk_size ] for x in range( 0, len( nyse_common_stock_historical_file_list ), chunk_size ) ]
        amex_common_stock_historical_file_chunk_list = [ amex_common_stock_historical_file_list[ x : x + chunk_size ] for x in range( 0, len( amex_common_stock_historical_file_list ), chunk_size ) ]

        renamed_ohlcv_column_list = [ 'Open', 'High', 'Low', 'Close', 'Volume' ]
        src_historical_data_column_list = [ '<TICKER>', '<DATE>', '<OPEN>', '<HIGH>', '<LOW>', '<CLOSE>', '<VOL>' ]
        renamed_src_historical_data_column_list = [ 'Ticker', 'Date' ] + renamed_ohlcv_column_list

        print( 'Start Extracting Stooq Historical Data...' )

        for nasdaq_common_stock_historical_file_chunk_no, nasdaq_common_stock_historical_file_chunk in enumerate( nasdaq_common_stock_historical_file_chunk_list ):
            chunk_export_start_time = time.time()

            for nasdaq_common_stock_historical_file_no, nasdaq_common_stock_historical_file in enumerate( nasdaq_common_stock_historical_file_chunk ):
                nasdaq_common_stock_historical_data_df = pd.read_csv( nasdaq_common_stock_historical_file, usecols=src_historical_data_column_list )
                ticker_symbol = nasdaq_common_stock_historical_data_df.iloc[ 0 ].values[ 0 ].split( '.' )[ 0 ]

                nasdaq_common_stock_historical_data_df.index = pd.to_datetime( nasdaq_common_stock_historical_data_df.loc[ :, '<DATE>' ], format='%Y%m%d' )
                nasdaq_common_stock_historical_data_df.index.name = 'Date'
                nasdaq_common_stock_historical_data_df.columns = pd.MultiIndex.from_product( [ [ ticker_symbol ], renamed_src_historical_data_column_list ] ) 
                nasdaq_common_stock_historical_data_df = nasdaq_common_stock_historical_data_df.loc[ start_date:datetime.datetime.now().strftime( '%Y-%m-%d' ), idx[ :, renamed_ohlcv_column_list ] ]

                if nasdaq_common_stock_historical_file_no == 0:
                    full_nasdaq_common_stock_historical_data_df = nasdaq_common_stock_historical_data_df
                else:
                    full_nasdaq_common_stock_historical_data_df = pd.concat( [ full_nasdaq_common_stock_historical_data_df, nasdaq_common_stock_historical_data_df ], axis=1 )

            nasdaq_historical_data_chunk_file_dir = os.path.join( root_dir, 'Historical/Stooq/Nasdaq/nasdaq_historical_data_chunk_' + str( nasdaq_common_stock_historical_file_chunk_no + 1 ) + '.csv' )
            full_nasdaq_common_stock_historical_data_df.to_csv( nasdaq_historical_data_chunk_file_dir )
            log_msg( 'Nasdaq Common Stock Historical Data Chunk %s Size: %s, Export Time, %s seconds' % ( ( nasdaq_common_stock_historical_file_chunk_no + 1 ), len( nasdaq_common_stock_historical_file_chunk ), ( time.time() - chunk_export_start_time ) ) )

        log_msg( "Nasdaq Common Stock Historical Data Extract Time, %s seconds" % ( time.time() - start_time ) )

        for nyse_common_stock_historical_file_chunk_no, nyse_common_stock_historical_file_chunk in enumerate( nyse_common_stock_historical_file_chunk_list ):
            chunk_export_start_time = time.time()

            for nyse_common_stock_historical_file_no, nyse_common_stock_historical_file in enumerate( nyse_common_stock_historical_file_chunk ):
                nyse_common_stock_historical_data_df = pd.read_csv( nyse_common_stock_historical_file, usecols=src_historical_data_column_list )
                ticker_symbol = nyse_common_stock_historical_data_df.iloc[ 0 ].values[ 0 ].split( '.' )[ 0 ]

                nyse_common_stock_historical_data_df.index = pd.to_datetime( nyse_common_stock_historical_data_df.loc[ :, '<DATE>' ], format='%Y%m%d' )
                nyse_common_stock_historical_data_df.index.name = 'Date'
                nyse_common_stock_historical_data_df.columns = pd.MultiIndex.from_product( [ [ ticker_symbol ], renamed_src_historical_data_column_list ] ) 
                nyse_common_stock_historical_data_df = nyse_common_stock_historical_data_df.loc[ start_date:datetime.datetime.now().strftime( '%Y-%m-%d' ), idx[ :, renamed_ohlcv_column_list ] ]

                if nyse_common_stock_historical_file_no == 0:
                    nyse_full_common_stock_historical_data_df = nyse_common_stock_historical_data_df
                else:
                    nyse_full_common_stock_historical_data_df = pd.concat( [ nyse_full_common_stock_historical_data_df, nyse_common_stock_historical_data_df ], axis=1 )

            nyse_historical_data_chunk_file_dir = os.path.join( root_dir, 'Historical/Stooq/Nyse/nyse_historical_data_chunk_' + str( nyse_common_stock_historical_file_chunk_no + 1 ) + '.csv' )
            nyse_full_common_stock_historical_data_df.to_csv( nyse_historical_data_chunk_file_dir )
            log_msg( 'Nyse Common Stock Historical Data Chunk %s Size: %s, Export Time, %s seconds' % ( ( nyse_common_stock_historical_file_chunk_no + 1 ), len( nyse_common_stock_historical_file_chunk ), ( time.time() - chunk_export_start_time ) ) )

        log_msg( 'Nyse Common Stock Historical Data Extract Time, %s seconds' % ( time.time() - start_time ) )

        for amex_common_stock_historical_file_chunk_no, amex_common_stock_historical_file_chunk in enumerate( amex_common_stock_historical_file_chunk_list ):
            chunk_export_start_time = time.time()

            for amex_common_stock_historical_file_no, amex_common_stock_historical_file in enumerate( amex_common_stock_historical_file_chunk ):
                amex_common_stock_historical_data_df = pd.read_csv( amex_common_stock_historical_file, usecols=src_historical_data_column_list )
                ticker_symbol = amex_common_stock_historical_data_df.iloc[ 0 ].values[ 0 ].split( '.' )[ 0 ]

                amex_common_stock_historical_data_df.index = pd.to_datetime( amex_common_stock_historical_data_df.loc[ :, '<DATE>' ], format='%Y%m%d' )
                amex_common_stock_historical_data_df.index.name = 'Date'
                amex_common_stock_historical_data_df.columns = pd.MultiIndex.from_product( [ [ ticker_symbol ], renamed_src_historical_data_column_list ] ) 
                amex_common_stock_historical_data_df = amex_common_stock_historical_data_df.loc[ start_date:datetime.datetime.now().strftime( '%Y-%m-%d' ), idx[ :, renamed_ohlcv_column_list ] ]

                if amex_common_stock_historical_file_no == 0:
                    amex_full_common_stock_historical_data_df = amex_common_stock_historical_data_df
                else:
                    amex_full_common_stock_historical_data_df = pd.concat( [ amex_full_common_stock_historical_data_df, amex_common_stock_historical_data_df ], axis=1 )

            amex_historical_data_chunk_file_dir = os.path.join( root_dir, 'Historical/Stooq/Amex/amex_historical_data_chunk_' + str( amex_common_stock_historical_file_chunk_no + 1 ) + '.csv' )
            amex_full_common_stock_historical_data_df.to_csv( amex_historical_data_chunk_file_dir )
            log_msg( 'Amex Common Stock Historical Data Chunk %s Size: %s, Export Time, %s seconds' % ( ( amex_common_stock_historical_file_chunk_no + 1 ), len( amex_common_stock_historical_file_chunk ), ( time.time() - chunk_export_start_time ) ) )

        log_msg( "Amex Common Stock Historical Data Extract Time, %s seconds" % ( time.time() - start_time ) )
    except Exception as e:
        logging.exception( 'Stooq Historical Data Extract Failed, Cause: %s' % e )
        raise Exception( 'Stooq Historial Data Extract Error' )

def download_historical_data_from_yfinance( exchange: str ) -> dict:
    start_time = time.time()
    end_date = datetime.datetime.now().strftime( '%Y-%m-%d' )

    ticker_info_map = get_ticker_chunk_list( chunk_size )
    nasdaq_ticker_chunks = ticker_info_map[ 'nasdaq_ticker_chunks' ]
    other_ticker_chunks = ticker_info_map[ 'other_ticker_chunks' ]

    nasdaq_historical_data_folder_dir = os.path.join( root_dir, 'Historical/Yfinance/Nasdaq/' )
    other_historical_data_folder_dir = os.path.join( root_dir, 'Historical/Yfinance/Others/' )
    multi_thread = True

    try:
        if exchange == 'NQ':
            for nasdaq_chunk_no, nasdaq_ticker_chunk in enumerate( nasdaq_ticker_chunks ):
                chunk_download_start_time = time.time()
                try:
                    chunk_file_dir = nasdaq_historical_data_folder_dir + 'nasdaq_historical_data_chunk_' + str( nasdaq_chunk_no + 1 ) + '.csv'
                    print( 'Downloading...' )
                    print( 'Multi Thread Mode: %s' % multi_thread )
                    data = yf.download( nasdaq_ticker_chunk, start=start_date, end=end_date, group_by = 'ticker', threads=multi_thread )
                    log_msg( 'Nasdaq Stock Historical Data Chunk %(chunk_no)s Download Completed, Chunk Size: %(chunk_size)s' % { 'chunk_no':( nasdaq_chunk_no + 1 ), 'chunk_size':len( nasdaq_ticker_chunk ) } )
                    data.to_csv( chunk_file_dir )
                    log_msg( 'Nasdaq Stock Historical Data Chunk %s CSV Download and Export Time, %s seconds' % ( ( nasdaq_chunk_no + 1 ), ( time.time() - chunk_download_start_time ) ) )
                except Exception as e:
                    log_msg( 'Nasdaq Stock Historical Data Chunk %s Download Failed' % ( nasdaq_chunk_no + 1 ) )
                    logging.exception( 'Nasdaq Stock Historical Data Chunk %s Download Failed, Cause: %s' % ( ( nasdaq_chunk_no + 1 ), e ) )
                    raise Exception( 'Nasdaq Stock Historical Data Download Error' )
        else:
            for other_chunk_no, other_ticker_chunk in enumerate( other_ticker_chunks ):
                chunk_download_start_time = time.time()
                try:
                    chunk_file_dir = other_historical_data_folder_dir + 'other_historical_data_chunk_' + str( other_chunk_no + 1 ) + '.csv'
                    print( 'Downloading...' )
                    print( 'Multi Thread Mode: %s' % multi_thread )
                    data = yf.download( other_ticker_chunk, start=start_date, end=end_date, group_by = 'ticker', threads=multi_thread )
                    log_msg( 'Other Stocks\' Historical Data Chunk %(chunk_no)s Download Completed, Chunk Size: %(chunk_size)s' % { 'chunk_no':( other_chunk_no + 1 ), 'chunk_size':len( other_ticker_chunk ) } )
                    data.to_csv( chunk_file_dir )
                    log_msg( 'Other Stocks\' Historical Data Chunk %s CSV Download and Export Time, %s seconds' % ( ( other_chunk_no + 1 ), ( time.time() - chunk_download_start_time ) ) )
                except Exception as e:
                    log_msg( 'Other Stocks\' Historical Data Chunk %s Download Failed' % ( other_chunk_no + 1 ) )
                    logging.exception( 'Other Stocks\' Historical Data Chunk %s Download Failed, Cause: %s' % ( ( other_chunk_no + 1 ), e ) )
                    raise Exception( 'Other Stocks\' Historical Data Download Error' )

        log_msg( "Historical Data Download, Exchange: %s, Time, %s seconds" % ( exchange, ( time.time() - start_time ) ) )
    except Exception as e:
        logging.exception( 'Historical Data Download From Yfinance Failed, Cause: %s' % e )
        raise Exception( 'Historical Data Download From Yfinance Error' )

def load_src_data_dataframe( src_folder_list: list ):
    start_time = time.time()
    data_file_path_list = []
    data_df_list = []

    try:
        for download_folder_dir in src_folder_list:
            temp_data_file_path_list = [ os.path.join( download_folder_dir, data_file ) for data_file in os.listdir( download_folder_dir ) if os.path.isfile( os.path.join( download_folder_dir, data_file ) ) ]
            data_file_path_list.extend( temp_data_file_path_list )
        
        data_df_list = [ pd.read_csv( data_file_path, header=[ 0, 1 ], index_col=0 ) for data_file_path in data_file_path_list ]
        print( "Source DataFrame Loading Time, %s seconds" % ( time.time() - start_time ) )
    except Exception as e:
        logging.exception( 'Source DataFrame Loading Failed, Cause: %s' % e )
        raise Exception( 'Source DataFrame Loading Error' )
    
    return data_df_list

def append_custom_indicators( src_data_dataframe_list: list ):
    start_time = time.time()
    
    historical_data_df_list = load_src_data_dataframe( src_data_dataframe_list )
    
    try:
        for index, historical_data_df in enumerate( historical_data_df_list ):
            append_start_time = time.time()
            
            #Close Change
            close_change_df = historical_data_df.loc[ :, idx[ :, 'Close' ] ].pct_change().mul( 100 ).rename( columns={ 'Close':'Close Change' } )
            result_df = pd.concat( [ historical_data_df, close_change_df ], axis=1 )

            #High Change
            high_change_df = historical_data_df.loc[ :, idx[ :, 'High' ] ].pct_change().mul( 100 ).rename( columns={ 'High':'High Change' } )
            result_df = pd.concat( [ result_df, high_change_df ], axis=1 )

            #Candle Upper/ Lower Body
            open_df = historical_data_df.loc[ :, idx[ :, 'Open' ] ].rename( columns={ 'Open': 'Compare' } )
            close_df = historical_data_df.loc[ :, idx[ :, 'Close' ] ].rename( columns={ 'Close': 'Compare' } )

            close_above_open_boolean_df = ( close_df > open_df )
            close_above_open_df = close_df.where( close_above_open_boolean_df.values )
            open_above_close_df = open_df.where( ( ~close_above_open_boolean_df ).values )
            upper_body_df = close_above_open_df.fillna( open_above_close_df ).rename( columns={ 'Compare': 'Upper Body' } )
            result_df = pd.concat( [ result_df, upper_body_df ], axis=1 )

            open_below_close_df = open_df.where( close_above_open_boolean_df.values )
            close_below_open_df = close_df.where( ( ~close_above_open_boolean_df ).values )
            lower_body_df = open_below_close_df.fillna( close_below_open_df ).rename( columns={ 'Compare': 'Lower Body' } )
            result_df = pd.concat( [ result_df, lower_body_df ], axis=1 )

            #Volume Change
            vol_change_df = historical_data_df.loc[ :, idx[ :, 'Volume' ] ].pct_change().mul( 100 ).rename( columns={ 'Volume':'Vol Change' } )
            result_df = pd.concat( [ result_df, vol_change_df ], axis=1 )

            #Closing Price Moving Average
            sma_5_close_df = historical_data_df.loc[ :, idx[ :, 'Close' ] ].rolling( window=5 ).mean().rename( columns={ 'Close':'5MA Close' } )
            result_df = pd.concat( [ result_df, sma_5_close_df ], axis=1 )
            sma_20_close_df = historical_data_df.loc[ :, idx[ :, 'Close' ] ].rolling( window=20 ).mean().rename( columns={ 'Close':'20MA Close' } )
            result_df = pd.concat( [ result_df, sma_20_close_df ], axis=1 )
            sma_50_close_df = historical_data_df.loc[ :, idx[ :, 'Close' ] ].rolling( window=50 ).mean().rename( columns={ 'Close':'50MA Close' } )
            result_df = pd.concat( [ result_df, sma_50_close_df ], axis=1 )
            sma_150_close_df = historical_data_df.loc[ :, idx[ :, 'Close' ] ].rolling( window=150 ).mean().rename( columns={ 'Close':'150MA Close' } )
            result_df = pd.concat( [ result_df, sma_150_close_df ], axis=1 )
            sma_200_close_df = historical_data_df.loc[ :, idx[ :, 'Close' ] ].rolling( window=200 ).mean().rename( columns={ 'Close':'200MA Close' } )
            result_df = pd.concat( [ result_df, sma_200_close_df ], axis=1 )

            #Volume Moving Average
            sma_5_volume_df = historical_data_df.loc[ :, idx[ :, 'Volume' ] ].rolling( window=5 ).mean().rename( columns={ 'Volume':'5MA Vol' } )
            result_df = pd.concat( [ result_df, sma_5_volume_df ], axis=1 )
            sma_20_volume_df = historical_data_df.loc[ :, idx[ :, 'Volume' ] ].rolling( window=20 ).mean().rename( columns={ 'Volume':'20MA Vol' } )
            result_df = pd.concat( [ result_df, sma_20_volume_df ], axis=1 )
            sma_50_volume_df = historical_data_df.loc[ :, idx[ :, 'Volume' ] ].rolling( window=50 ).mean().rename( columns={ 'Volume':'50MA Vol' } )
            result_df = pd.concat( [ result_df, sma_50_volume_df ], axis=1 )

            '''#Difference Between Different Moving Average
            diff_5_sma_and_20_sma_close = ( ( result_df.loc[ :, idx[ :, '5MA Close' ] ].sub( result_df.loc[ :, idx[ :, '20MA Close' ] ].values, axis=1 ) ).div( result_df.loc[ :, idx[ :, '20MA Close' ] ].values, axis=1 ) ).mul( 100 ).rename( columns={ '5MA Close':'5MA/20MA Diff' } )
            result_df = pd.concat( [ result_df, diff_5_sma_and_20_sma_close ], axis=1 )
            diff_20_sma_50_sma_close = ( ( result_df.loc[ :, idx[ :, '20MA Close' ] ].sub( result_df.loc[ :, idx[ :, '50MA Close' ] ].values, axis=1 ) ).div( result_df.loc[ :, idx[ :, '50MA Close' ] ].values, axis=1 ) ).mul( 100 ).rename( columns={ '20MA Close':'20MA/50MA Diff' } )
            result_df = pd.concat( [ result_df, diff_20_sma_50_sma_close ], axis=1 )
            diff_150_sma_50_sma_close = ( ( result_df.loc[ :, idx[ :, '50MA Close' ] ].sub( result_df.loc[ :, idx[ :, '150MA Close' ] ].values, axis=1 ) ).div( result_df.loc[ :, idx[ :, '150MA Close' ] ].values, axis=1 ) ).mul( 100 ).rename( columns={ '50MA Close':'50MA/150MA Diff' } )
            result_df = pd.concat( [ result_df, diff_150_sma_50_sma_close ], axis=1 )'''

            '''#MACD
            ema_12_close_df = historical_data_df.loc[ :, idx[ :, 'Close' ] ].ewm( span=12, adjust=False ).mean()
            ema_26_close_df = historical_data_df.loc[ :, idx[ :, 'Close' ] ].ewm( span=26, adjust=False ).mean()
            diff_df = ( ema_12_close_df.sub( ema_26_close_df.values ) ).rename( columns={ 'Close':'DIFF' } )
            dea_df = diff_df.ewm( span=9, adjust=False ).mean().rename( columns={ 'DIFF':'DEA' } )
            macd_df = ( diff_df.sub( dea_df.values ) ).mul( 2 ).rename( columns={ 'DIFF':'MACD' } )
            result_df = pd.concat( [ result_df, diff_df ], axis=1 )
            result_df = pd.concat( [ result_df, dea_df ], axis=1 )
            result_df = pd.concat( [ result_df, macd_df ], axis=1 )

            #RSI (5, 10, 14)
            close_diff_df = historical_data_df.loc[ :, idx[ :, 'Close' ] ].diff()
            up_df = close_diff_df.clip( lower=0 )
            down_df = -1 * close_diff_df.clip( upper=0 )

            rsi_5_ema_up = up_df.ewm( com=4, adjust=False ).mean()
            rsi_5_ema_down = down_df.ewm( com=4, adjust=False ).mean()
            rs_5_df = rsi_5_ema_up/ rsi_5_ema_down
            rsi_5_df = ( 100 - ( 100/ ( 1 + rs_5_df ) ) ).rename( columns={ 'Close':'RSI 5' } )

            rsi_10_ema_up = up_df.ewm( com=9, adjust=False ).mean()
            rsi_10_ema_down = down_df.ewm( com=9, adjust=False ).mean()
            rs_10_df = rsi_10_ema_up/ rsi_10_ema_down
            rsi_10_df = ( 100 - ( 100/ ( 1 + rs_10_df ) ) ).rename( columns={ 'Close':'RSI 10' } )

            rsi_14_ema_up = up_df.ewm( com=13, adjust=False ).mean()
            rsi_14_ema_down = down_df.ewm( com=13, adjust=False ).mean()
            rs_14_df = rsi_14_ema_up/ rsi_14_ema_down
            rsi_14_df = ( 100 - ( 100/ ( 1 + rs_14_df ) ) ).rename( columns={ 'Close':'RSI 14' } )

            result_df = pd.concat( [ result_df, rsi_5_df ], axis=1 )
            result_df = pd.concat( [ result_df, rsi_10_df ], axis=1 )
            result_df = pd.concat( [ result_df, rsi_14_df ], axis=1 )'''

            log_msg( 'Chunk %s Append Custom Statistics Time, %s seconds' % ( ( index + 1 ), ( time.time() - append_start_time ) ) )

            if DataSourceType.STOOQ.name == historical_data_source_type:
                export_dir = os.path.join( root_dir, ( 'Historical/Stooq/Full/full_historical_data_chunk_' + str( index + 1 ) + '.csv' ) )
            elif DataSourceType.YFINANCE.name == historical_data_source_type:
                export_dir = os.path.join( root_dir, ( 'Historical/Yfinance/Full/full_historical_data_chunk_' + str( index + 1 ) + '.csv' ) )
            result_df.to_csv( export_dir )

        log_msg( 'Append and Export Time, %s seconds' % ( time.time() - start_time ) )
    except Exception as e:
        logging.exception( 'Append and Export Failed, Cause: %s' % e )
        raise Exception( 'Append and Export Error' )

def get_stock_chart_by_ticker_list( ticker_list, filter_condition_dict_list ):
    start_time = time.time()
    result_list = []
    try:
        print( 'Number of Stock Charts: %s' % len( ticker_list ) )
        for ticker in ticker_list:
            #Redirect to https://charts2.finviz.com/chart.ashx?t={ticker}&ty=c&ta=1&p=d
            url = f'https://finviz.com/chart.ashx?t={ticker}&ty=c&ta=1&p=d'
            result_list.append( url )
        
        chart_folder_dir = os.path.join( root_dir, 'Charts/' )
        current_date = datetime.datetime.now().strftime( '%Y%m%d' )
        current_time = datetime.datetime.now().strftime( '%H%M%S' )
        export_folder_name = current_date + current_time

        export_folder_dir = os.path.join( chart_folder_dir, export_folder_name )
        export_file_dir = os.path.join( export_folder_dir, 'chart.html' )
        os.mkdir( export_folder_dir )

        no_of_result_html = '<div style="font-size:23px; font-weight: bold; margin-bottom: 50px;">No. of Filtered Result:'
        no_of_result_html += str( len( ticker_list ) )
        no_of_result_html += '</div>'

        with open( export_file_dir, 'w' ) as f:
            f.write( no_of_result_html )
            for item in result_list:
                f.write( '<img style="width:900px; height:300px" src=\"%s\"/>\n' % item )
            for filter_condition in filter_condition_dict_list:
                f.write( '<div>%s</div>' % filter_condition )

        print( "--- Get Stock Chart Time, %s seconds ---" % ( time.time() - start_time ) )
    except Exception as e:
        logging.exception( 'Get Stock Chart Failed, Cause: %s' % e )
        raise Exception( 'Get Stock Chart Error' )