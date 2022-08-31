"""MySQL Utilities"""
import mysql.connector
import json
from dotenv import dotenv_values
from cache import redis_client


config = dotenv_values(".env")

mysql_quotes = mysql.connector.connect(host=config['MYSQL_QUOTES_IP_DB_PUBLIC'],
                                       user=config['MYSQL_QUOTES_USER'],
                                       password=config['MYSQL_QUOTES_PASSWORD'],
                                       database=config['MYSQL_QUOTES_DATABASE'])

mysql_wealth = mysql.connector.connect(host=config['MYSQL_WEALTH_IP_DB_PUBLIC'],
                                       user=config['MYSQL_WEALTH_USER'],
                                       password=config['MYSQL_WEALTH_PASSWORD'],
                                       database=config['MYSQL_WEALTH_DATABASE'])


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

    redis_client.set(basket, json.dumps(symbols), ex=60 * 60 * 24)

    return symbols


def get_watchlist(watchlist: str):
    """get watchlist from basket"""
    if watchlist is None:
        return []

    cursor = mysql_wealth.cursor()
    cursor.execute(
        "SELECT ti.id_tag as symbol "
        "FROM dataindex.watchlist w "
        "JOIN dataindex.tagindice ti ON (w.id_watchlist = ti.id_indice AND w.id_tabella = ti.id_tabella) "
        "WHERE w.codesterno = %s;",
        (watchlist,))
    symbols = cursor.fetchall()
    cursor.close()

    return [symbol[0] for symbol in symbols]
