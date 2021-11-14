from constant.pattern import Pattern
from constant.filter_criteria import FilterCriteria
from model.pattern.abcd import Abcd

class PatternFilterFactory:
    @staticmethod
    def get_filter(pattern, historical_data_df: list, filter_criteria_dict: dict):
        pattern = filter_criteria_dict.get(FilterCriteria.PATTERN)

        if Pattern.ABCD == pattern:
            return Abcd(historical_data_df, filter_criteria_dict)
        else:
            raise Exception('Filter Pattern of {pattern} Not Found')
        