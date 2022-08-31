"""module for interacting with aws s3"""
import boto3
import json
import io
import pandas as pd
from dotenv import dotenv_values
from cache import redis_client

config = dotenv_values(".env")

s3 = boto3.client(
    's3',
    aws_access_key_id=config['AWS_ACCESS_KEY_ID'],
    aws_secret_access_key=config['AWS_SECRET_ACCESS_KEY'],
    region_name=config['AWS_DEFAULT_REGION']
)


def get_file(fx: str, date: str, timeframe: int):
    """get file from s3"""
    key = f'{fx}/{date}/{timeframe}.parquet'
    cache = redis_client.get(key)
    if cache is not None:
        return json.loads(cache)

    resp = s3.get_object(Bucket=config['AWS_BUCKET_NAME'], Key=key)

    if resp['ContentLength'] == 0:
        return 'File not found'
    if resp['Body'] is None:
        return 'File is empty'

    body = resp['Body'].read()

    buffer = io.BytesIO(body)
    result = pd.read_parquet(buffer).query('fx == @fx and timeframe == @timeframe')

    scanner_list = json.loads(result.to_json(orient='records'))
    # for scanner in scanner_list:
    #     scanner['data'] = json.loads(scanner['data'].replace("'", '"'))

    redis_client.set(key, json.dumps(scanner_list), ex=60)
    return scanner_list


def put_file(key, data):
    """put file to s3"""
    resp = s3.put_object(Bucket=config['AWS_BUCKET_NAME'], Key=key, Body=data, ContentType='application/x-parquet')
    return resp
