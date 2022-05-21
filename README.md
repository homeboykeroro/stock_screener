# stock_screnner
PROJECT SETUP:

1. Run py -m venv VENV_NAME
2. Run pip install -r requirements.txt
3. Set STOCK_DATA_ROOT_FOLDER_DIR and HISTORICAL_DATA_SOURCE_TYPE in config.json


HOW TO USE:

1. Download historical data zip file from stooq.com
2. Place the zip file in STOCK_DATA_ROOT_FOLDER_DIR and unzip
3. Run historical_data_initialiser.py to extract historical data from Stooq source files (skip the step if it has already been done before)
4. Run chat_pattern_screener.py
5. Type in filter condition in JSON format (templates can be found in filter_condition_templates.txt)


FUNCTION:
- Filter stocks based on chart pattern including unusual volume gap up