import json
import time
import os
import asyncio

from unicorn_binance_websocket_api.unicorn_binance_websocket_api_manager \
    import BinanceWebSocketApiManager

from utils import get_logger_from_self
from workers.constants import IS_CHECKED_FIELDNAME, TRADE_ID_FIELD, \
    TRADE_SYMBOL_FIELD, TRADE_TIMESTAMP_FIELD
from workers.data_base import BinanceDataStreamBase


class BinanceWebSocketReceiver(BinanceDataStreamBase):
    def __init__(self, symbols, streams, loop=None, gather_len=100):
        super().__init__(loop)
        self.bm = BinanceWebSocketApiManager(exchange="binance.com")

        self.symbols = self._get_symbols(symbols)
        self.streams = streams
        self.gather_len = gather_len

        self.parsing_trade_columns = {
            TRADE_SYMBOL_FIELD: 's', TRADE_ID_FIELD: 't', 'price': 'p',
            'quantity': 'q', TRADE_TIMESTAMP_FIELD: 'T',
            'is_buyer_market_maker': 'm'}

        self.logger = get_logger_from_self(self)
        self.log_dir = './logs'
        self.buffer_filename = os.path.join(self.log_dir, 'buffer_len.log')
        self.buffer_critical_len = 100000

    def _get_symbols(self, symbols):
        if isinstance(symbols, str) and symbols == 'all':
            return self._get_all_symbols()
        elif isinstance(symbols, str) and symbols.split('_')[0] == 'top':
            return self._get_top_n_symbols(symbols)
        else:
            return symbols

    def _get_all_symbols(self):
        exchange_info = self.binance_client.get_exchange_info()
        return [symbol[TRADE_SYMBOL_FIELD] for symbol in exchange_info[
            'symbols'] if symbol['status'] == 'TRADING']

    def _get_top_n_symbols(self, symbols):
        n = int(symbols.split('_')[1])
        tickers_info = self.binance_client.get_ticker()
        topn_tickers = sorted(
            tickers_info, key=lambda x: x['count'], reverse=True)[:n]
        return [ticker['symbol'] for ticker in topn_tickers]

    async def execute_tasks(self, tasks):
        if len(tasks) == 0:
            return
        await asyncio.gather(*tasks)
        tasks.clear()

    async def start_websocket(self):
        self.bm.create_stream(self.streams, self.symbols)

        message = f'Websocket connection opened for ' \
                  f'{self.mongo_manager.db_client.address}...'
        self._send_log_info(message, log_level='info')
        tasks = []
        start = time.time()
        try:
            last_buffer_excel = False
            while True:
                if len(tasks) >= self.gather_len:
                    num_tasks = len(tasks)
                    num_new_messages = len(self.bm.stream_buffer)
                    await self.execute_tasks(tasks)
                    new_messages = len(self.bm.stream_buffer) - \
                        num_new_messages
                    elapsed = time.time() - start
                    message = f'One message process time is ' \
                              f'{elapsed / num_tasks}'
                    self._send_log_info(message, log_level='debug',
                                        to_telegram=False)
                    self._save_buffer_len(elapsed, num_tasks, new_messages)
                    start = time.time()

                msg = self.bm.pop_stream_data_from_stream_buffer()

                current_buffer_excel = len(
                    self.bm.stream_buffer) > self.buffer_critical_len
                if last_buffer_excel != current_buffer_excel:
                    message = f'Your stream buffer is ' \
                              f'{len(self.bm.stream_buffer)} len'
                    self._send_log_info(message, log_level='warning')
                    last_buffer_excel = current_buffer_excel

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

                    if stream.split('@')[-1].lower() == 'trade':
                        tasks.append(
                            asyncio.create_task(
                                self._process_trade_ticker(msg)))
                    else:
                        tasks.append(
                            asyncio.create_task(
                                self._process_book_ticker(msg)))

        except Exception as e:
            message = f'Uncaught exception: {e}'
            self._send_log_info(message, log_level='exception')
            raise e

    async def _process_trade_ticker(self, msg):
        if msg['e'] == 'error':
            message = f'Error occurred at processing trade ticker:\n {msg}'
            self._send_log_info(message, log_level='error')
            return msg

        new_document = self._parse_msg(msg)
        collection = await self.mongo_manager.init_collection(
            new_document[TRADE_SYMBOL_FIELD], msg['e'])
        query = {TRADE_ID_FIELD: new_document[TRADE_ID_FIELD]}
        await self.mongo_manager.update(collection, query, new_document)

    @staticmethod
    async def _process_book_ticker(msg):
        print(msg)

    def _parse_msg(self, msg):
        parsed = self._base_parse_trade(msg)
        parsed[IS_CHECKED_FIELDNAME] = False
        return parsed

    def _save_buffer_len(self, elapsed, num_tasks, new_messages):
        message = f"Buffer size = {str(len(self.bm.stream_buffer))}\n" \
                  f"Elapsed {elapsed:0.2f} sec. for " \
                  f"{str(num_tasks)} operations\n" \
                  f"New messages number = {str(new_messages)}\n"
        with open(self.buffer_filename, 'w') as file:
            file.write(message)
