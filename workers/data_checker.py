import logging
import time
from glob import glob
from collections import defaultdict


class BinanceDataChecker:
    def __init__(self, sleep_time=12, root_dir='./data'):
        # Given in hours
        self.sleep_time = sleep_time * 60 * 60
        self.root_dir = root_dir
        self.trades_dir = f'{root_dir}/trades'
        self.order_book_dir = f'{root_dir}/order_book'
        self.logger = logging.getLogger('BinanceWebSocketReceiver_logger')

    def start_checking(self):
        while True:
            self._check_trades()
            self._check_order_books()
            time.sleep(self.sleep_time)

    def _check_trades(self):
        query = f'{self.trades_dir}/*/*.csv'
        all_files = glob(query)
        self._are_trades_sequential(all_files)
        for file in all_files:
            result = self._process_trade_file(file)
            if result:
                self._delete_old_file(file)

    def _are_trades_sequential(self, all_files):
        symbol_to_files = defaultdict(list)
        for file in all_files:
            symbol = file.split('/')[-2].lower()
            symbol_to_files[symbol].append(file)

        for symbol, files in symbol_to_files.items():
            last_file = None
            for file in files:
                if last_file is None:
                    last_file = file.split('/')[-1][:-4]
                    continue
                else:
                    last_value, step = map(int, last_file.split('_')[1:3])
                    print(0)

        print(0)

    def _process_trade_file(self, file):
        return True

    def _delete_old_file(self, file):
        pass

    def _check_order_books(self):
        pass
