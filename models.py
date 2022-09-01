"""MySQL Utilities"""
import json
import os
import mysql.connector
from pymongo import MongoClient
from cache import redis_client

MYSQL_CACHE_TIME=int(os.getenv('MYSQL_CACHE_TIME')) or 60 * 60


mysql_quotes = mysql.connector.connect(host=os.getenv('MYSQL_QUOTES_IP_DB_PRIVATE')
                                            if os.getenv('MYSQL_PRIVATE_IP') == '1'
                                            else os.getenv('MYSQL_QUOTES_IP_DB_PUBLIC'),
                                        user=os.getenv('MYSQL_QUOTES_USER'),
                                        password=os.getenv('MYSQL_QUOTES_PASSWORD'),
                                        database=os.getenv('MYSQL_QUOTES_DATABASE'))

mysql_wealth = mysql.connector.connect(host=os.getenv('MYSQL_WEALTH_IP_DB_PRIVATE')
                                            if os.getenv('MYSQL_PRIVATE_IP') == '1'
                                            else os.getenv('MYSQL_WEALTH_IP_DB_PUBLIC'),
                                        user=os.getenv('MYSQL_WEALTH_USER'),
                                        password=os.getenv('MYSQL_WEALTH_PASSWORD'),
                                        database=os.getenv('MYSQL_WEALTH_DATABASE'))

mongo_client = MongoClient(os.getenv('MONGO_DB_CONN_STRING'))
indicators = mongo_client[os.getenv('INDICATOR_DB_NAME')] \
                            .get_collection(os.getenv('INDICATOR_COLLECTION_NAME'))

def get_symbols(basket: int):
    """get symbols from basket"""
    if basket is None or not isinstance(basket, int):
        basket = 1000

    cache = redis_client.get(basket)
    if cache is not None:
        return json.loads(cache)

    cursor = mysql_quotes.cursor()
    cursor.execute('SELECT l.symbol FROM valori.lista l WHERE l.idpaniere = %s', (basket,))
    symbols = cursor.fetchall()
    cursor.close()

    if len(symbols) == 0:
        return []

    symbols = [symbol[0] for symbol in symbols]

    redis_client.set(basket, json.dumps(symbols), ex=MYSQL_CACHE_TIME)

    return symbols


def get_watchlist(watchlist: str):
    """get watchlist from basket"""
    if watchlist is None:
        return []

    cursor = mysql_wealth.cursor()
    cursor.execute(
        "SELECT ti.id_tag as symbol "
        "FROM dataindex.watchlist w "
        "JOIN dataindex.tagindice ti "
        "ON (w.id_watchlist = ti.id_indice AND w.id_tabella = ti.id_tabella) "
        "WHERE w.codesterno = %s;",
        (watchlist,))
    symbols = cursor.fetchall()
    cursor.close()

    if len(symbols) == 0:
        return []

    return [symbol[0] for symbol in symbols]


def get_indicator_all_fxs() -> list:
    """get all indicator FXs"""
    item_details = indicators.aggregate([
        {'$match': {'timeframe': {'$in': [100000, 200000, 300000]}}},
        {'$group': {'_id': {"fx": "$fx"}}}
    ])
    
    return [item['_id']['fx'] for item in item_details]


def get_indicators(code: str, timeframe: int) -> list:
    """get indicators from fx and timeframe"""
    records = indicators.aggregate([
        {'$match': {'fx': code, 'timeframe': timeframe}},
        {'$project':
            {
                '_id': 0,
                'symbol': 1,
                'code': 1,
                'time': 1,
                'timeframe': 1,
                'last_upd': 1,
                'fx': code,
                'data':
                '$data'
            }
        }
    ])
    
    return records
