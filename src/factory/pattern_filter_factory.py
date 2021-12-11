from constant.pattern import Pattern
from constant.filter_criteria import FilterCriteria
from pattern.abcd_consolidation import AbcdConsolidation
from pattern.unusual_ramp_up import UnusualRampUp

class PatternFilterFactory:
    @staticmethod
    def get_filter(historical_data_df: list, filter_criteria_dict: dict):
        pattern = filter_criteria_dict.get(FilterCriteria.PATTERN)

        if Pattern.ABCD_CONSOLIDATION == pattern:
            return AbcdConsolidation(historical_data_df, filter_criteria_dict)
        elif Pattern.UNUSUAL_RAMP_UP == pattern:
            return UnusualRampUp(historical_data_df, filter_criteria_dict)
        else:
            raise Exception('Filter Pattern of {pattern} Not Found')
        