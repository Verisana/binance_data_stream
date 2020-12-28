import logging
import json
import os
import datetime
from glob import glob

import telegram
from unicorn_binance_websocket_api.unicorn_binance_websocket_api_manager import BinanceWebSocketApiManager
from telegram import Bot
from dotenv import load_dotenv
from binance.client import Client


class BinanceWebSocketReceiver:
    def __init__(self, symbols=None, streams=None, trades_in_file=1000000,
                 root_dir='./data'):
        self.bm = BinanceWebSocketApiManager(exchange="binance.com")
        self.binance_client = Client('', '')
        self.symbols = symbols or self._get_all_symbols()
        self.streams = streams or ['trade']
        load_dotenv()
        token = os.getenv('BOT_TOKEN')
        self.chat_id = os.getenv('CHAT_ID')

        self.bot = Bot(token)
        self.logger = logging.getLogger('BinanceWebSocketReceiver_logger')

        # Константы
        self.data_trade_filename = '{}_{}_{}.csv'

        self.columns_order = {'symbol': 's', 'trade_id': 't', 'price': 'p',
                              'quantity': 'q', 'trade_time': 'T',
                              'is_buyer_market_maker': 'm'}
        self.trades_in_file = trades_in_file
        self.root_dir = root_dir
        self.trades_dir = os.path.join(root_dir, 'trades')
        self.order_book_dir = os.path.join(root_dir, 'order_book')
        self._prepare_folders()

    def _prepare_folders(self):
        if not os.path.exists(self.root_dir):
            os.makedirs(self.root_dir)
        if not os.path.exists(self.trades_dir):
            os.makedirs(self.trades_dir)
        if not os.path.exists(self.order_book_dir):
            os.makedirs(self.order_book_dir)

        for symbol in self.symbols:
            trade_symbol_dir = os.path.join(self.trades_dir, symbol.upper())
            order_book_symbol_dir = os.path.join(self.order_book_dir, symbol.upper())
            if not os.path.exists(trade_symbol_dir):
                os.makedirs(trade_symbol_dir)
            if not os.path.exists(order_book_symbol_dir):
                os.makedirs(order_book_symbol_dir)

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

    def _signal_if_alive(self, counter, last_hour):
        now = datetime.datetime.now()
        if (now.hour % 3 == 0) and (last_hour != now.hour):
            message = f'Execution stream reached {counter} operations milestone'
            self.logger.info(message)
            self.bot.send_message(self.chat_id, message)
            last_hour = now.hour
        counter += 1
        return counter, last_hour

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

    def _create_empty_symbol_file(self, file_path, filename_trade):
        message = f"Creating new file {filename_trade}"
        self._send_log_info(message, log_level='info', to_telegram=False)
        with open(file_path, 'w') as file:
            headers = ','.join(self.columns_order.keys())
            headers += '\n'
            file.write(headers)

    @staticmethod
    def _append_new_trade(file_path, to_save):
        with open(file_path, 'a') as file:
            file.write(to_save)

    def _process_trade_ticker(self, msg):
        if msg['e'] == 'error':
            message = f'Error occurred at processing trade ticker:\n {msg}'
            self._send_log_info(message, log_level='error')
            return msg

        new_document = self._parse_msg(msg)

        root_dir = os.path.join(self.trades_dir, new_document["symbol"])
        all_files = sorted(glob(os.path.join(root_dir, '*')))
        if all_files:
            # Находим начало trade_id у последнего файла
            last_file_trade_id = int(all_files[-1].split('_')[1])
        else:
            last_file_trade_id = new_document['trade_id']

        range_in_file = range(last_file_trade_id, last_file_trade_id + self.trades_in_file)

        filename_trade = None
        if new_document['trade_id'] in range_in_file:
            filename_trade = self.data_trade_filename.format(
                new_document['symbol'], last_file_trade_id,
                self.trades_in_file)
        elif new_document['trade_id'] == last_file_trade_id+1:
            filename_trade = self.data_trade_filename.format(
                new_document['symbol'], last_file_trade_id+1,
                self.trades_in_file)
        else:
            message = f"Lost trades from " \
                      f"{last_file_trade_id+self.trades_in_file} to " \
                      f"{new_document['trade_id']}"
            self._send_log_info(message, log_level='info')
            for i in range(
                    last_file_trade_id+self.trades_in_file,
                    new_document['trade_id']+1, self.trades_in_file):
                filename_trade = self.data_trade_filename.format(
                    new_document['symbol'], i, self.trades_in_file)
                file_path = os.path.join(root_dir, filename_trade)
                self._create_empty_symbol_file(file_path, filename_trade)
            if filename_trade is None:
                filename_trade = self.data_trade_filename.format(
                    new_document['symbol'], last_file_trade_id+1,
                    self.trades_in_file)

        to_save = self._get_str_to_save(new_document)
        file_path = os.path.join(root_dir, filename_trade)
        if os.path.exists(file_path):
            self._append_new_trade(file_path, to_save)
        else:
            self._create_empty_symbol_file(file_path, filename_trade)
            self._append_new_trade(file_path, to_save)

    @staticmethod
    def _process_book_ticker(msg):
        print(msg)

    def _parse_msg(self, msg):
        return {k: msg[v] for k, v in self.columns_order.items()}

    def _get_str_to_save(self, new_document):
        new_line = ''
        for column in self.columns_order.keys():
            new_line += str(new_document[column]) + ','

        # Меняем последнюю запятую на перенос строки
        new_line = new_line[:-1]
        new_line += '\n'

        return new_line
