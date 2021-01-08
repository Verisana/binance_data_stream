import os

import pymongo
from pymongo import MongoClient
from pymongo.errors import PyMongoError

from utils import get_logger_from_self, BaseLogger
from workers.constants import TRADE_ID_FIELD, TRADE_PARSED_TIME_FIELD


class MongoManager(BaseLogger):
    def __init__(self, connect_cred=None):
        super().__init__()
        self.db_client, self.db = self.init_mongodb_connection(connect_cred)
        self.logger = get_logger_from_self(self)

    def init_collection(self, symbol, stream):
        collection = self.get_db_collection(self.db, symbol, stream)
        collection.create_index(
            [(TRADE_ID_FIELD, pymongo.ASCENDING),
             (TRADE_PARSED_TIME_FIELD, pymongo.ASCENDING)], unique=True)
        return collection

    def update(self, collection, query, document, upsert=True, is_one=True):
        func = collection.update_one if is_one else collection.update_many
        args = (query, {'$set': document})
        kwargs = {'upsert': upsert}
        return self._execute_operation(func, *args, **kwargs)

    def insert_many(self, collection, documents, trade_id, diff):
        query = {TRADE_ID_FIELD: {
            '$in': list(range(int(trade_id), int(trade_id+diff)))}}
        existing_trades = self._execute_operation(collection.find, query)
        existing_trades = [doc[TRADE_ID_FIELD] for doc in existing_trades]
        documents = list(filter(lambda x: x[TRADE_ID_FIELD] not in
                                existing_trades, documents))
        return self._execute_operation(
            collection.insert_many, documents) if len(documents) > 0 else None

    @staticmethod
    def init_mongodb_connection(connect_cred=None):
        user = os.getenv('MONGO_INITDB_ROOT_USERNAME')
        password = os.getenv('MONGO_INITDB_ROOT_PASSWORD')
        host = f"{os.getenv('MONGO_HOST')}:27017/"
        connect_cred = f"mongodb://{user}:{password}@{host}" \
            if connect_cred is None else connect_cred
        client = MongoClient(connect_cred)
        db = client.get_database(os.getenv('MONGO_DBNAME'))
        return client, db

    @staticmethod
    def get_db_collection(db, symbol, stream):
        collection_name = f"{symbol.upper()}_{stream.lower()}"
        return db.get_collection(collection_name)

    def _execute_operation(self, func, *args, **kwargs):
        try:
            result = func(*args, **kwargs)
        except PyMongoError as e:
            message = f"PyMongoError: {e}"
            self._send_log_info(message, 'error')
            return

        if not isinstance(result, pymongo.cursor.Cursor) and \
                not result.acknowledged:
            message = f"Couldn't proceed {func} with {args} and {kwargs}. " \
                      f"Check result: {result.raw_result}"
            self._send_log_info(message, 'error')
        return result
