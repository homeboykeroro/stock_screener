from constant.pattern import Pattern
from constant.datasource_type import DataSourceType
from datasource.stooq_datasource import StooqDataSource
from model.pattern.ascending_triangle import AscendingTriange

class PatternFilterFactory:
    @staticmethod
    def get_filter(pattern, historical_data_list, *args, **kwargs):
        if Pattern.ASCENDING_TRIANGLE == pattern:
            return AscendingTriange(historical_data_list)