import functools
import pickle
from typing import Callable

from src.cache import Cache
from redis import Redis


class RedisLRUCache(Cache):
    def __init__(self, redis_host: str, redis_port: int, max_size: int, cached_chunk_size):
        self.redis_client = Redis(redis_host, redis_port, decode_responses=True)
        self.hset_name = "lru_cache"
        self.chached_chunk_size =
        self.max_size = max_size

    def get(self, key):
        return self.redis_client.hget(self.hset_name, key)

    def put(self, key, value):
        return self.redis_client.hset(self.hset_name, key, value)

    def delete(self, key):
        return self.redis_client.hdel(self.hset_name, key)

    def __call__(self, func: Callable):
        @functools.wraps(func)
        def func_wrapper(*args, **kwargs):
            key = pickle.dumps((func.__name__, args, kwargs))
            value = self.get(key)
            if value:
                print("received from cache essekititttttt")
                return value
            else:
                print("received from function's execution")
                result = func(*args, **kwargs)
                self.put(key, result)
                return result
        return func_wrapper

