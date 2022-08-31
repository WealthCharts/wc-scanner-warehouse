"""cache"""
import os
import redis

url = os.getenv('REDIS_URL')
port = os.getenv('REDIS_PORT')

redis_client = redis.Redis(host=url, port=port)
