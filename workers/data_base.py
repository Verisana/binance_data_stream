import os
from datetime import datetime

from binance.client import Client

from utils import BaseLogger, get_logger_from_self
from workers.constants import TRADE_ID_FIELD, TRADE_TIMESTAMP_FIELD, \
    TRADE_PARSED_TIME_FIELD
from workers.mongo_manager import MongoManager


class BinanceDataStreamBase(BaseLogger):
    def __init__(self):
        super().__init__()
        self.binance_client = Client(os.getenv('BINANCE_API'),
                                     os.getenv('BINANCE_TOKEN'))
        self.mongo_manager = MongoManager()

        self.parsing_trade_columns = {
            'symbol': 's', TRADE_ID_FIELD: 't', 'price': 'p', 'quantity': 'q',
            TRADE_TIMESTAMP_FIELD: 'T', 'is_buyer_market_maker': 'm'}
        self.logger = get_logger_from_self(self)

    def _base_parse_trade(self, data):
        parsed = {k: data[v] for k, v in self.parsing_trade_columns.items()}
        parsed[TRADE_PARSED_TIME_FIELD] = datetime.utcfromtimestamp(
            parsed[TRADE_TIMESTAMP_FIELD] / 1000)
        return parsed
