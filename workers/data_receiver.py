import logging
import json
import time

import pymongo
from pymongo.errors import PyMongoError
from unicorn_binance_websocket_api.unicorn_binance_websocket_api_manager \
    import BinanceWebSocketApiManager
from dotenv import load_dotenv

from utils import get_db_collection
from workers.constants import IS_CHECKED_FIELDNAME, TRADE_ID_FIELD, \
    TRADE_PARSED_TIME_FIELD
from workers.data_base import BinanceDataStreamBase

load_dotenv()


class BinanceWebSocketReceiver(BinanceDataStreamBase):
    def __init__(self, symbols, streams):
        super().__init__()
        self.bm = BinanceWebSocketApiManager(exchange="binance.com")

        self.symbols = self._get_all_symbols() if symbols == 'all' else symbols
        self.streams = streams

        self.logger = logging.getLogger('BinanceWebSocketReceiver_logger')

    def _get_all_symbols(self):
        exchange_info = self.binance_client.get_exchange_info()
        return [symbol['symbol'] for symbol in exchange_info[
            'symbols'] if symbol['status'] == 'TRADING']

    def start_websocket(self):
        self.bm.create_stream(self.streams, self.symbols)

        message = f'Websocket connection opened for ' \
                  f'{self.db_client.address}...'
        self._send_log_info(message, log_level='info')
        try:
            last_buffer_excel = False
            while True:
                msg = self.bm.pop_stream_data_from_stream_buffer()
                if msg:
                    start = time.time()
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

                    if stream.split('@')[-1].lower() == 'trade':
                        self._process_trade_ticker(msg)
                    else:
                        self._process_book_ticker(msg)
                    message = f'One message process time is ' \
                              f'{time.time() - start}'
                    self._send_log_info(message, log_level='debug')
                else:
                    time.sleep(0.3)

                if len(self.bm.stream_buffer) > 10000:
                    current_buffer_excel = True
                else:
                    current_buffer_excel = False

                if last_buffer_excel != current_buffer_excel:
                    message = f'Your stream buffer is ' \
                              f'{len(self.bm.stream_buffer)} len'
                    self._send_log_info(message, log_level='warning')
                    last_buffer_excel = current_buffer_excel
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

        collection = get_db_collection(self.db, new_document['symbol'],
                                       msg['e'])
        collection.create_index(
            [(TRADE_ID_FIELD, pymongo.ASCENDING),
             (TRADE_PARSED_TIME_FIELD, pymongo.ASCENDING)], unique=True)

        try:
            result = collection.update_one({
                TRADE_ID_FIELD: new_document['trade_id']},
                {'$set': new_document}, upsert=True)
        except PyMongoError as e:
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
        parsed = self._base_parse_trade(msg)
        parsed[IS_CHECKED_FIELDNAME] = False
        return parsed
