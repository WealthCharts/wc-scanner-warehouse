"""http server for scanner API"""
# pylint: disable=E1101
# pylint: disable=E1121
import json
import os
import time
from datetime import date
from flask import Flask, jsonify, request
import load_env
from cache import redis_client
import models
import service

URL_CACHE_TIME=int(os.getenv('URL_CACHE_TIME')) or 60 * 60


application = Flask(__name__)



@application.before_request
def check_headers_api_key():
    """check api key"""
    if request.path != '/' and request.headers.get('X-API-KEY') != os.getenv('API_KEY'):
        return jsonify({'error': 'Invalid API key'}), 401


@application.before_request
def before_request_func():
    """before request"""
    if request.path != '/' and request.method == 'GET':
        cache = redis_client.get(request.url)
        if cache is not None:
            return json.loads(cache)


@application.after_request
def after_request_func(response):
    """after request"""
    if request.method == 'GET':
        string = response.get_data(as_text=True)
        redis_client.set(request.url, string, ex=URL_CACHE_TIME)
    return response

@application.route('/', methods=['GET'])
def index():
    """index"""
    return jsonify({'message': 'Welcome to the scanner API'}), 200



@application.route('/<scanner_fx>/<timeframe>', methods=['POST'])
def scanner_post(scanner_fx: str,  timeframe: int):
    """sync scanner"""
    today = date.today().strftime('%Y%m%d')
    data = models.get_indicators(scanner_fx, timeframe)
    service.put_file(scanner_fx, today, timeframe, list(data))
    time.sleep(3)
    return 'Synced'


@application.route('/<code>/<date>/<timeframe>', methods=['GET'])
def scanner_get(code: str, scanner_date: str, timeframe: int):
    """return scanner results"""
    url = request.url
    cache = redis_client.get(url)
    if cache is not None:
        return json.loads(cache)

    timeframe = int(timeframe)
    watchlist = request.args.get('watchlist') if request.args.get('watchlist') else None
    basket = int(request.args.get('basket')) if request.args.get('basket') else None

    if int(scanner_date) > 20200101:
        return jsonify({'error': 'invalid date'})

    if basket is None or not isinstance(basket, int):
        basket = 1000

    if timeframe not in [100000, 200000, 300000]:
        timeframe = 100000

    result = service.get_file(code, scanner_date, timeframe)

    response: list = []
    if basket is not None and isinstance(basket, int):
        symbols = models.get_symbols(basket)
        for row in result:
            if row['symbol'] in symbols:
                row['data'] = json.loads(row['data'].replace("'", '"'))
                response.append(row)
        redis_client.set(url, json.dumps(response), ex=URL_CACHE_TIME)
        return response

    if watchlist is not None and isinstance(watchlist, int):
        watchlist = models.get_watchlist(watchlist)
        for row in result:
            if row['symbol'] in watchlist:
                row['data'] = json.loads(row['data'].replace("'", '"'))
                response.append(row)
        redis_client.set(url, json.dumps(response), ex=URL_CACHE_TIME)
        return response

    for row in result:
        row['data'] = json.loads(row['data'].replace("'", '"'))
        response.append(row)

    redis_client.set(url, json.dumps(response), ex=URL_CACHE_TIME)
    return response


if __name__ == '__main__':
    if os.environ.get('FLASK_DEBUG') == '1' or load_env.LOCAL:
        application.run(debug=True, host='localhost', port=int(os.environ.get('PORT', 5000)))
    else:
        application.run(debug=False, port=int(os.environ.get('PORT', 5000)))
