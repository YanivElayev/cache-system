import functools
import pickle
import time
from typing import Callable, List

from src.cache import Cache
from redis import Redis

from src.constants import SMALL_REQUEST_SIZE, LARGE_REQUEST_SIZE


class RedisLRUChunksCache(Cache):
    def __init__(self, redis_host: str, redis_port: int, max_size: int, chunk_sizes: List[int]):
        self.redis_client = Redis(redis_host, redis_port, decode_responses=True)
        self.chunk_sizes = chunk_sizes
        self.access_times_sorted_sets = {chunk_size: f"{chunk_size}_access_times" for chunk_size in chunk_sizes}
        self.max_size = max_size

    def get(self, offset: int, size: int):
        self.redis_client.zadd(self.access_times_sorted_sets[size], {str(offset): time.time()})
        # TODO don't update temporary size's access time
        return self.redis_client.hget(str(size), str(offset))

    def put(self, offset: int, value: str):
        current_size = 0
        for chunk_size in self.chunk_sizes:
            current_size += chunk_size*self.redis_client.hlen(str(chunk_size))
        size = len(value)
        if current_size + size > self.max_size:
            print("deleting older chunks")
            offset_to_remove = self.redis_client.zrange(self.access_times_sorted_sets[size], 0, 0)[0]
            # TODO try remove from temporary cache sizes first.
            self.delete(offset_to_remove, size)
        self.redis_client.zadd(self.access_times_sorted_sets[size], {str(offset): time.time()})
        return self.redis_client.hset(str(size), str(offset), value)

    def delete(self, offset: int, size: int):
        self.redis_client.zrem(self.access_times_sorted_sets[size], offset)
        return self.redis_client.hdel(str(size), str(offset))

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
                if len(result) == SMALL_REQUEST_SIZE:
                    self.put(offset, result)
                    large_chunk_from_requested_offset = func(offset, LARGE_REQUEST_SIZE)
                    self.put(offset, large_chunk_from_requested_offset)
                return result

        return func_wrapper
