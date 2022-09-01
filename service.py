"""module for interacting with aws s3"""
import json
import io
import os
import time
from datetime import date
import boto3
import pandas as pd
from cache import redis_client
import models

s3 = boto3.client(
    's3',
    aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
    region_name=os.getenv('AWS_DEFAULT_REGION')
)

def parquet_to_json(buffer):
    """convert parquet to json"""
    dataframe = pd.read_parquet(buffer)
    return dataframe.to_json(orient='records')


def json_to_parquet(json_data: object):
    """convert json to parquet"""
    string = json.dumps(json_data)
    buffer = io.BytesIO(string.encode('utf-8'))
    dataframe = pd.read_json(buffer, orient='records')
    dataframe.columns = dataframe.columns.astype(str)
    
    for col in dataframe.columns:
        if dataframe[col].dtype == 'object':
            dataframe[col] = dataframe[col].astype(str)
    return dataframe.to_parquet()


def get_file(code: str, date: str, timeframe: int):
    """get file from s3"""
    key = f'{code}/{date}/{timeframe}.parquet'
    cache = redis_client.get(key)
    if cache is not None:
        return json.loads(cache)

    resp = s3.get_object(Bucket=os.getenv('AWS_BUCKET_NAME'), Key=key)

    if resp['ContentLength'] == 0:
        return 'File not found'
    if resp['Body'] is None:
        return 'File is empty'

    body = resp['Body'].read()

    buffer = io.BytesIO(body)
    result = pd.read_parquet(buffer).query('fx == @code and timeframe == @timeframe')

    scanner_list = json.loads(result.to_json(orient='records'))

    redis_client.set(key, json.dumps(scanner_list), ex=60)
    return scanner_list


def put_file(code: str, date: int, timeframe: int, data: list):
    """put file to s3"""
    key = f'{code}/{date}/{timeframe}.parquet'
    data = json_to_parquet(data)
    resp = s3.put_object(
        Bucket=os.getenv('AWS_BUCKET_NAME'),
        Key=key,
        Body=data,
        ContentType='application/x-parquet')
    return resp

def sync_all():
    scanners_fx = models.get_indicator_all_fxs()
    scanners_timeframe = [100000, 200000, 300000]

    # today format: YYYYMMDD
    today = date.today().strftime('%Y%m%d')
    
    for scanner_fx in scanners_fx:
        for scanner_timeframe in scanners_timeframe:
            data = models.get_indicators(scanner_fx, scanner_timeframe)
            put_file(scanner_fx, today, scanner_timeframe, list(data))
            # wait for s3 to be ready
            time.sleep(3)
    return 'Synced'