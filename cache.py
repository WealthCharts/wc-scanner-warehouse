"""cache"""
import os
import redis

redis_client = redis.Redis(host=os.getenv('REDIS_URL'), port=os.getenv('REDIS_PORT'))
