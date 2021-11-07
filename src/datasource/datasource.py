from abc import ABC, abstractmethod

from pandas.core.frame import DataFrame

class DataSource(ABC):
    @abstractmethod
    def initialise_historical_data(self) -> None:
        return NotImplemented
    
    @abstractmethod
    def get_historical_data_df_list(self) -> DataFrame:
        return NotImplemented