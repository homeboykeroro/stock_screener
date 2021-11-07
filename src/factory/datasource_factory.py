from constant.datasource_type import DataSourceType
from datasource.stooq_datasource import StooqDataSource

class DataSourceFactory:
    @staticmethod
    def get_datasource(data_source_type: str):
        if DataSourceType.STOOQ == data_source_type:
            return StooqDataSource()