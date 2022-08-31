"""http server for scanner API"""
import json

from flask import Flask, jsonify, request
from cache import redis_client

import models
import s3

app = Flask(__name__)


@app.route('/', methods=['GET'])
def index():
    """returns a string"""
    return 'Hello World!'


@app.route('/<fx>/<date>/<timeframe>', methods=['GET'])
def scanner(fx: str, date: str, timeframe: int):
    """return scanner results"""
    url = request.url
    cache = redis_client.get(url)
    if cache is not None:
        return json.loads(cache)

    timeframe = int(timeframe)
    watchlist = request.args.get('watchlist') if request.args.get('watchlist') else None
    basket = int(request.args.get('basket')) if request.args.get('basket') else None

    # check if date is valid format YYYYMMDD
    if len(date) != 8:
        return jsonify({'error': 'invalid date'})
    # if not date.split('-')[0].isdigit() or not date.split('-')[1].isdigit() or not date.split('-')[2].isdigit():
    #     return jsonify({'error': 'invalid date'})

    if basket is None or not isinstance(basket, int):
        basket = 1000

    if timeframe not in [100000, 200000, 300000]:
        timeframe = 100000

    result = s3.get_file(fx, date, timeframe)

    response: list = []
    if basket is not None and isinstance(basket, int):
        symbols = models.get_symbols(basket)
        for row in result:
            if row['symbol'] in symbols:
                row['data'] = json.loads(row['data'].replace("'", '"'))
                response.append(row)
        redis_client.set(url, json.dumps(response), ex=60)
        return response

    if watchlist is not None and isinstance(watchlist, int):
        watchlist = models.get_watchlist(watchlist)
        for row in result:
            if row['symbol'] in watchlist:
                row['data'] = json.loads(row['data'].replace("'", '"'))
                response.append(row)
        redis_client.set(url, json.dumps(response), ex=60)
        return response

    for row in result:
        row['data'] = json.loads(row['data'].replace("'", '"'))
        response.append(row)

    redis_client.set(url, json.dumps(response), ex=60)
    return response


if __name__ == '__main__':
    app.run(debug=True)
