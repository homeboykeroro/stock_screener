import time
import json

from config.config import config, ConfigProperty
from constant.filter_criteria import FilterCriteria

from factory.datasource_factory import DataSourceFactory
from factory.pattern_filter_factory import PatternFilterFactory

from utils.log_util import get_logger
from utils.stock_data_util import get_stock_chart

logger = get_logger()

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
                
                logger.debug(f'--- Filter By {pattern}, {time.time() - start_time} seconds ---')
        except Exception as e:
            logger.exception('Filter Failed, Cause:')

filter_stocks()