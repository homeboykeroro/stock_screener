# stock_screnner
PROJECT INITIALIZATION STEPS:

1. Run py -m venv VENV_NAME
2. Run pip install -r requirements.txt
3. Set 'STOCK_DATA_ROOT_FOLDER_DIR' to 'Project Directory/Data' in config.json


HOW TO USE:

1. Run historical_data_initialiser.py to download historical data from Yahoo Finance ( skip the step if data is downloaded before )
2. Run chat_pattern_screener.py
3. Type in filter condition in JSON format ( templates can be found in filter_condition_templates.txt )