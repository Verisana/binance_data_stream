import logging
import os
from datetime import datetime

import telegram
from telegram import Bot
from dotenv import load_dotenv
from binance.client import Client

from utils import init_mongodb_connection
from workers.constants import TRADE_ID_FIELD, TRADE_TIMESTAMP_FIELD, \
    TRADE_PARSED_TIME_FIELD

load_dotenv()


class BinanceDataStreamBase:
    def __init__(self):
        self.binance_client = Client(os.getenv('BINANCE_API'),
                                     os.getenv('BINANCE_TOKEN'))
        self.db_client, self.db = init_mongodb_connection()

        self.chat_id = os.getenv('CHAT_ID')
        token = os.getenv('BOT_TOKEN')
        self.bot = Bot(token)

        self.logger = logging.getLogger('BinanceDataStreamBase_logger')

        self.parsing_trade_columns = {
            'symbol': 's', TRADE_ID_FIELD: 't', 'price': 'p', 'quantity': 'q',
            TRADE_TIMESTAMP_FIELD: 'T', 'is_buyer_market_maker': 'm'}

    def _send_log_info(self, message, log_level='info', to_telegram=True):
        if log_level == 'debug':
            self.logger.debug(message)
        elif log_level == 'info':
            self.logger.info(message)
        elif log_level == 'error':
            self.logger.error(message)
        elif log_level == 'exception':
            self.logger.exception(message, exc_info=True)
        if to_telegram:
            try:
                self.bot.send_message(self.chat_id, message, timeout=3)
            except telegram.error.TimedOut as e:
                self.logger.info(f"TimedOut error {e} to Telegram while "
                                 f"sending message {message}")

    def _base_parse_trade(self, data):
        parsed = {k: data[v] for k, v in self.parsing_trade_columns.items()}
        parsed[TRADE_PARSED_TIME_FIELD] = datetime.utcfromtimestamp(
            parsed[TRADE_TIMESTAMP_FIELD] / 1000)
        return parsed
