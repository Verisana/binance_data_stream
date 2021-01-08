import time
from datetime import datetime
from typing import List

import numpy as np
from binance.exceptions import BinanceAPIException

from utils import get_logger_from_self
from workers.constants import IS_CHECKED_FIELDNAME, TRADE_ID_FIELD, \
    TRADE_TIMESTAMP_FIELD, TRADE_SYMBOL_FIELD
from workers.data_base import BinanceDataStreamBase


class BinanceDataChecker(BinanceDataStreamBase):
    def __init__(self, sleep_time=5):
        super().__init__()

        # Given in minutes
        self.sleep_time = sleep_time * 60
        self.logger = get_logger_from_self(self)

        self.parsing_trade_columns = {
            TRADE_SYMBOL_FIELD: TRADE_SYMBOL_FIELD, TRADE_ID_FIELD: 'id',
            'price': 'price', 'quantity': 'qty', TRADE_TIMESTAMP_FIELD: 'time',
            'is_buyer_market_maker': 'isBuyerMaker'}

    def start_checking(self):
        while True:
            start = time.time()
            trades_added, documents_count = self._check_trades()
            message = f"Data consistency checked in " \
                      f"{time.time() - start:0.2f} sec.\nAdded " \
                      f"{trades_added} missed trades\nChecked " \
                      f"{documents_count} existing documents"
            if trades_added > 0:
                self._send_log_info(message)
            else:
                self._send_log_info(message, to_telegram=False)

            self._check_order_books()
            time.sleep(self.sleep_time)

    def _check_trades(self):
        all_collections = self.mongo_manager.db.list_collection_names()
        trade_collections = list(filter(lambda x: x.endswith('trade'),
                                 all_collections))
        trades_added, documents_count = 0, 0
        for collection in trade_collections:
            missing_trades, docs_count = self._process_trade_collection(
                self.mongo_manager.db[collection])
            documents_count += docs_count
            trades_added += missing_trades
        if documents_count == len(trade_collections):
            message = f"No new updates across all collections. " \
                      f"Check Data Receiver"
            self._send_log_info(message, log_level='warning')
        return trades_added, documents_count

    def _process_trade_collection(self, collection):
        all_documents = list(collection.find(
            {IS_CHECKED_FIELDNAME: False}, [TRADE_ID_FIELD]).sort('trade_id'))
        if len(all_documents) <= 1:
            return 0, len(all_documents)
        trade_ids = np.array([doc[TRADE_ID_FIELD] for doc in all_documents])
        diffs = np.diff(trade_ids)
        all_trades_filled = 0
        checked_trades = []
        for trade_id, diff, document in zip(trade_ids[:-1], diffs,
                                            all_documents[:-1]):
            trade_id = int(trade_id)
            if diff == 1:
                checked_trades.append(trade_id)
            else:
                trades_filled = self._fill_missing_docs(collection,
                                                        diff-1, trade_id+1)
                all_trades_filled += trades_filled
                if trades_filled > 0:
                    checked_trades.append(trade_id)
        if len(checked_trades) > 0:
            query = {TRADE_ID_FIELD: {"$in": checked_trades}}
            self.mongo_manager.update(
                collection, query, {IS_CHECKED_FIELDNAME: True}, is_one=False)
        return all_trades_filled, len(all_documents)

    def _fill_missing_docs(self, collection, diff, trade_id):
        symbol = collection.name.split('_')[0]
        missing_data = self._fetch_missing_data(symbol, diff, trade_id)
        if len(missing_data) == 0:
            return 0
        else:
            result = self.mongo_manager.insert_many(collection, missing_data,
                                                    trade_id, diff)
            return 0 if result is None else len(missing_data)

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
        results = {}
        for i in range(2): # two attempts
            try:
                results = self.binance_client.get_historical_trades(
                    symbol=symbol, limit=limit, fromId=from_id)
                # Add 'symbol' key for later parsing
                _ = list(map(lambda x: x.update({TRADE_SYMBOL_FIELD: symbol}),
                         results))
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
        return results

    def _parse_trade_data(self, datas: List[dict]):
        result = []
        for data in datas:
            parsed = self._base_parse_trade(data)
            parsed[IS_CHECKED_FIELDNAME] = False
            result.append(parsed)
        return sorted(result, key=lambda x: x[TRADE_ID_FIELD])

    def _check_order_books(self):
        pass
