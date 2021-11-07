from enum import Enum

class StockExchange(str, Enum):
    NASDAQ = 'NASDAQ',
    NYSE = 'NYSE',
    AMEX = 'AMEX'