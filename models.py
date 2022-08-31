"""MySQL Utilities"""
import json
import os
import mysql.connector
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

    return [symbol[0] for symbol in symbols]
