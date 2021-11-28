from constant.pattern import Pattern
from constant.filter_criteria import FilterCriteria
from pattern.abcd_consolidation import AbcdConsolidation

class PatternFilterFactory:
    @staticmethod
    def get_filter(historical_data_df: list, filter_criteria_dict: dict):
        pattern = filter_criteria_dict.get(FilterCriteria.PATTERN)

        if Pattern.ABCD == pattern:
            return AbcdConsolidation(historical_data_df, filter_criteria_dict)
        else:
            raise Exception('Filter Pattern of {pattern} Not Found')
        