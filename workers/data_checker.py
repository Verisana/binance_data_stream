import logging
import time
from typing import List

import numpy as np
from dotenv import load_dotenv
from binance.exceptions import BinanceAPIException

from workers.constants import IS_CHECKED_FIELDNAME, TRADE_ID_FIELD
from workers.data_base import BinanceDataStreamBase

load_dotenv()


class BinanceDataChecker(BinanceDataStreamBase):
    def __init__(self, sleep_time=1):
        super().__init__()

        # Given in hours
        self.sleep_time = sleep_time * 60 * 60
        self.logger = logging.getLogger('BinanceWebSocketReceiver_logger')

    def start_checking(self):
        while True:
            self._check_trades()
            self._check_order_books()
            time.sleep(self.sleep_time)

    def _check_trades(self):
        all_collections = self.db.list_collection_names()
        trade_collections = filter(lambda x: x.endswith('trade'),
                                   all_collections)
        for collection in trade_collections:
            self._process_trade_collection(self.db[collection])

    def _process_trade_collection(self, collection):
        all_documents = list(collection.find(
            {IS_CHECKED_FIELDNAME: False}, [TRADE_ID_FIELD]).sort('trade_id'))
        trade_ids = np.array([doc[TRADE_ID_FIELD] for doc in all_documents])
        diffs = np.diff(trade_ids)
        for trade_id, diff, document in zip(trade_ids, diffs, all_documents):
            if diff == 1:
                document[IS_CHECKED_FIELDNAME] = True
            else:
                is_filled = self._fill_missing_docs(collection,
                                                    diff-2, trade_id+1)
                if is_filled:
                    document[IS_CHECKED_FIELDNAME] = True

    def _fill_missing_docs(self, collection, diff, trade_id):
        symbol = collection.name.split('_')[0]
        missing_data = self._fetch_missing_data(symbol, diff, trade_id)
        if len(missing_data) == 0:
            return False
        else:
            pass

    def _fetch_missing_data(self, symbol, limit, from_id):
        set_last_unchecked = False
        all_data = []
        if limit <= 1000:
            all_data = self._get_historical_trades(symbol, limit, from_id)
        else:
            last_id = from_id + limit
            for current_from_id in range(from_id, from_id+limit, limit):
                new_limit = min(1000, last_id-current_from_id)
                new_data = self._get_historical_trades(symbol, new_limit,
                                                       current_from_id)
                if len(new_data) == 0:
                    if len(all_data) != 0:
                        set_last_unchecked = True
                        break
                else:
                    all_data.extend(new_data)

        all_data = self._parse_trade_data(all_data)
        if set_last_unchecked:
            all_data[-1][IS_CHECKED_FIELDNAME] = False
        return all_data

    def _get_historical_trades(self, symbol, limit, from_id):
        result = {}
        for i in range(2):
            try:
                result = self.binance_client.get_historical_trades(
                    symbol=symbol, limit=limit, fromId=from_id)
                break
            except BinanceAPIException as e:
                if e.code == -1003:
                    wait_secs = 90
                    message = f"Rate Limit Reached: {e}. Waiting {wait_secs} sec."
                    self._send_log_info(message, 'exception')
                    time.sleep(wait_secs)
                else:
                    message = f"Unexpected BinanceAPIException {e} while " \
                              f"fetching data for {symbol}, {limit}, {from_id}"
                    self._send_log_info(message, 'exception')
                    break
        return result

    def _parse_trade_data(self, datas: List[dict]):
        result = []
        for data in datas:
            parsed = self._base_parse_trade(data)
            parsed[IS_CHECKED_FIELDNAME] = True
            result.append(parsed)
        return sorted(result, key=lambda x: x[TRADE_ID_FIELD])

    def _check_order_books(self):
        pass
