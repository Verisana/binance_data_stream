import json
import time

from unicorn_binance_websocket_api.unicorn_binance_websocket_api_manager \
    import BinanceWebSocketApiManager

from utils import get_logger_from_self
from workers.constants import IS_CHECKED_FIELDNAME, TRADE_ID_FIELD, \
    SYMBOL_FIELD
from workers.data_base import BinanceDataStreamBase


class BinanceWebSocketReceiver(BinanceDataStreamBase):
    def __init__(self, symbols, streams):
        super().__init__()
        self.bm = BinanceWebSocketApiManager(exchange="binance.com")

        self.symbols = self._get_all_symbols() if symbols == 'all' else symbols
        self.streams = streams

        self.logger = get_logger_from_self(self)

    def _get_all_symbols(self):
        exchange_info = self.binance_client.get_exchange_info()
        return [symbol[SYMBOL_FIELD] for symbol in exchange_info[
            'symbols'] if symbol['status'] == 'TRADING']

    def start_websocket(self):
        self.bm.create_stream(self.streams, self.symbols)

        message = f'Websocket connection opened for ' \
                  f'{self.mongo_manager.db_client.address}...'
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
                    self._send_log_info(message, log_level='debug',
                                        to_telegram=False)
                else:
                    time.sleep(0.3)

                current_buffer_excel = len(self.bm.stream_buffer) > 1000
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
        collection = self.mongo_manager.init_collection(
            new_document[SYMBOL_FIELD], msg['e'])
        query = {TRADE_ID_FIELD: new_document[TRADE_ID_FIELD]}
        self.mongo_manager.update_one(collection, query, new_document)

    @staticmethod
    def _process_book_ticker(msg):
        print(msg)

    def _parse_msg(self, msg):
        parsed = self._base_parse_trade(msg)
        parsed[IS_CHECKED_FIELDNAME] = False
        return parsed
