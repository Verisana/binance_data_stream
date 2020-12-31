import logging
import json
import os
from datetime import datetime

import telegram
import pymongo
from unicorn_binance_websocket_api.unicorn_binance_websocket_api_manager import BinanceWebSocketApiManager
from telegram import Bot
from dotenv import load_dotenv
from binance.client import Client

from utils import init_mongodb_connection, get_collection
from workers.constants import IS_CHECKED_FIELDNAME

load_dotenv()


class BinanceWebSocketReceiver:
    def __init__(self, symbols, streams):
        self.bm = BinanceWebSocketApiManager(exchange="binance.com")
        self.binance_client = Client('', '')

        self.db_client, self.db = init_mongodb_connection()

        self.symbols = self._get_all_symbols() if symbols == 'all' else symbols
        self.streams = streams

        self.chat_id = os.getenv('CHAT_ID')
        token = os.getenv('BOT_TOKEN')
        self.bot = Bot(token)

        self.logger = logging.getLogger('BinanceWebSocketReceiver_logger')

        # Constants
        self.trade_id_field = 'trade_id'
        # Store original timestamp and parsed because of parsed loose data
        # while converting
        self.trade_timestamp_field = 'orig_trade_time'
        self.trade_parsed_time_field = 'parsed_trade_time'
        self.parsing_trade_columns = {
            'symbol': 's', self.trade_id_field: 't', 'price': 'p', 'quantity': 'q',
            self.trade_timestamp_field: 'T', 'is_buyer_market_maker': 'm'}

    def _get_all_symbols(self):
        exchange_info = self.binance_client.get_exchange_info()
        return [symbol['symbol'] for symbol in exchange_info[
            'symbols'] if symbol['status'] == 'TRADING']

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
                self.logger.info(f"TimedOut error to Telegram while sending "
                                 f"message {message}")

    def start_websocket(self):
        self.bm.create_stream(self.streams, self.symbols)

        message = 'Websocket connection opened...'
        self._send_log_info(message, log_level='info')

        try:
            while True:
                msg = self.bm.pop_stream_data_from_stream_buffer()
                if msg:
                    try:
                        msg = json.loads(msg)
                        stream = msg['stream']
                        msg = msg['data']
                    except json.JSONDecodeError:
                        message = f'Error occurred while decoding msg:\n {msg}'
                        self._send_log_info(message, log_level='exception')
                        continue

                    # Иногда вместо обновления какой-то мусор приходит
                    except KeyError:
                        continue

                    # counter, last_hour = self._signal_if_alive(counter, last_hour)
                    if stream.split('@')[-1].lower() == 'trade':
                        self._process_trade_ticker(msg)
                    else:
                        self._process_book_ticker(msg)

                if len(self.bm.stream_buffer) > 10000:
                    message = f'Your stream buffer is {len(self.bm.stream_buffer)} len'
                    self._send_log_info(message, log_level='warning')
        except Exception as e:
            message = f'Uncaught exception: {e}'
            self._send_log_info(message, log_level='exception')
            raise e

    def _process_trade_ticker(self, msg):
        if msg['e'] == 'error':
            message = f'Error occurred at processing trade ticker:\n {msg}'
            self._send_log_info(message, log_level='error')
            return msg

        new_document = self._parse_msg(msg)

        collection = get_collection(self.db, new_document['symbol'], msg['e'])
        collection.create_index(
            [(self.trade_id_field, pymongo.ASCENDING),
             (self.trade_parsed_time_field, pymongo.ASCENDING)], unique=True)

        try:
            result = collection.update_one({
                self.trade_id_field: new_document['trade_id']},
                {'$set': new_document}, upsert=True)
        except pymongo.errors.PyMongoError as e:
            message = f"PyMongoError: {e}"
            self._send_log_info(message, 'error')
            return

        if not result.acknowledged:
            message = f"Couldn't insert {new_document}. " \
                      f"Check result: {result.raw_result}"
            self._send_log_info(message, 'error')

    @staticmethod
    def _process_book_ticker(msg):
        print(msg)

    def _parse_msg(self, msg):
        parsed = {k: msg[v] for k, v in self.parsing_trade_columns.items()}
        parsed[self.trade_parsed_time_field] = datetime.utcfromtimestamp(
            parsed[self.trade_timestamp_field] / 1000)
        parsed[IS_CHECKED_FIELDNAME] = False
        return parsed
