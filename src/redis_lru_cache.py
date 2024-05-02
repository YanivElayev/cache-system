import functools
import pickle
import time
from typing import Callable

from src.cache import Cache
from redis import Redis

from src.constants import SMALL_REQUEST_SIZE, LARGE_REQUEST_SIZE, CACHE_MAX_SIZE


class RedisLRUChunksCache(Cache):
    def __init__(self, redis_host: str, redis_port: int, max_size: int):
        self.redis_client = Redis(redis_host, redis_port, decode_responses=True)
        self.cache_sets = {SMALL_REQUEST_SIZE: "lru_small_chunks_cache",
                           LARGE_REQUEST_SIZE: "lru_large_chunks_cache"}
        self.access_times_sorted_sets = {SMALL_REQUEST_SIZE: "small_chunks_access_scores",
                                         LARGE_REQUEST_SIZE: "large_chunks_access_scores"}
        self.max_size = max_size

    def get(self, offset, size):
        self.redis_client.zadd(self.access_times_sorted_sets[size], {offset: time.time()})
        return self.redis_client.hget(self.cache_sets[size], offset)

    def put(self, offset, value):
        current_size = SMALL_REQUEST_SIZE * self.redis_client.hlen(
            self.cache_sets[SMALL_REQUEST_SIZE]) + LARGE_REQUEST_SIZE * self.redis_client.hlen(
            self.cache_sets[LARGE_REQUEST_SIZE])
        size = len(value)
        if current_size + size > self.max_size:
            print("deleting older chunks")
            offset_to_remove = self.redis_client.zrange(self.access_times_sorted_sets[size], 0, 0)[0]
            self.delete(offset_to_remove, size)
        self.redis_client.zadd(self.access_times_sorted_sets[size], {offset: time.time()})
        return self.redis_client.hset(self.cache_sets[size], offset, value)

    def delete(self, offset, size):
        self.redis_client.zrem(self.access_times_sorted_sets[size], offset)
        return self.redis_client.hdel(self.cache_sets[size], offset)

    def __call__(self, func: Callable):
        @functools.wraps(func)
        def func_wrapper(offset: int, size: int):
            value = self.get(offset, size)
            if value:
                print("received from cache essekititttttt")
                return value
            else:
                print("received from function's execution")
                result = func(offset, size)
                self.put(offset, result)
                return result
        return func_wrapper
