import os

import pymongo
from pymongo import MongoClient
from pymongo.errors import PyMongoError, BulkWriteError

from utils import get_logger_from_self
from workers.data_base import BaseLogger
from workers.constants import TRADE_ID_FIELD, TRADE_PARSED_TIME_FIELD


class MongoManager(BaseLogger):
    def __init__(self):
        super().__init__()
        self.db_client, self.db = self.init_mongodb_connection()
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

    def insert_many(self, collection, documents):
        func = collection.insert_many
        return self._execute_operation(func, (documents))

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
        # except BulkWriteError as e:
        #     message = f"PyMongoError: {e}"
        #     self._send_log_info(message, 'error', to_telegram=False)
        #     return

        except PyMongoError as e:
            message = f"PyMongoError: {e}"
            self._send_log_info(message, 'error')
            return

        if not result.acknowledged:
            message = f"Couldn't proceed {func} with {args} and {kwargs}. " \
                      f"Check result: {result.raw_result}"
            self._send_log_info(message, 'error')
        return result
