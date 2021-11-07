from abc import ABC, abstractmethod

class PatternFilter(ABC):
    def __init__(self, historical_data_df_list: list, **kwargs) -> None:
        self.__historical_data_df_list = historical_data_df_list

    @abstractmethod
    def filter(self, **kwargs) -> list:
        return NotImplemented