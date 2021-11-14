import time
import json
import logging

from config.config import config, ConfigProperty
from constant.filter_criteria import FilterCriteria

from factory.datasource_factory import DataSourceFactory
from factory.pattern_filter_factory import PatternFilterFactory

from utils.common_util import log_msg
from utils.stock_data_utils import get_stock_chart

def filter_stocks():
    historical_data_source_type = config[ConfigProperty.HISTORICAL_DATA_SOURCE_TYPE]
    datasource = DataSourceFactory.get_datasource(historical_data_source_type)
    historical_data_df_list = datasource.get_historical_data_df_list()

    while True:
        try:
            filter_criteria_json = input('Input Filter Criteria: ')
            filter_criteria_dict_list = json.loads(filter_criteria_json)

            for filter_criteria_dict in filter_criteria_dict_list:
                start_time = time.time()
                pattern = filter_criteria_dict.get(FilterCriteria.PATTERN)

                pattern_filter = PatternFilterFactory.get_filter(historical_data_df_list, filter_criteria_dict)
                ticker_list = pattern_filter.filter()
                get_stock_chart(ticker_list, filter_criteria_dict)
                
                log_msg(f'--- Filter By {pattern}, {time.time() - start_time} seconds ---')
        except Exception as e:
            logging.exception(f'Filter Failed, Cause: {e}')

filter_stocks()