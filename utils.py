import os

from pymongo import MongoClient
from dotenv import load_dotenv

load_dotenv()


def get_standardized_str(str_to_convert):
    return str_to_convert.lower().strip('\n').strip(' ')


def init_mongodb_connection(connect_cred=None):
    user = os.getenv('MONGO_INITDB_ROOT_USERNAME')
    password = os.getenv('MONGO_INITDB_ROOT_PASSWORD')
    host = f"{os.getenv('MONGO_HOST')}:27017/"
    connect_cred = f"mongodb://{user}:{password}@{host}" \
        if connect_cred is None else connect_cred
    client = MongoClient(connect_cred)
    db = client.get_database(os.getenv('MONGO_DBNAME'))
    return client, db


def get_db_collection(db, symbol, stream):
    collection_name = f"{symbol.upper()}_{stream.lower()}"
    return db.get_collection(collection_name)
