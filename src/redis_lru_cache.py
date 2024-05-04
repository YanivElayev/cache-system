import functools
import pickle
import time
from typing import Callable, List

from src.cache import Cache
from redis import Redis

from src.constants import SECONDS_OF_LARGE_CHUNKS_IN_CACHE, LARGE_REQUEST_SIZE


class RedisLRUChunksCache(Cache):  # TODO rename since it is not a usual lru cache
    def __init__(self, redis_host: str, redis_port: int, chunk_sizes: List[int], max_size: int,
                 duration_of_largest_chunk_size=SECONDS_OF_LARGE_CHUNKS_IN_CACHE):
        self.redis_client = Redis(redis_host, redis_port, decode_responses=True)
        self.chunk_sizes = chunk_sizes
        self.access_times_sorted_sets = {chunk_size: f"{chunk_size}_access_times" for chunk_size in chunk_sizes}
        self.max_size = max_size
        self.largest_chunk_size = max(chunk_sizes)
        self.duration_of_largest_chunks = duration_of_largest_chunk_size

    def get(self, offset: int, size: int):
        if not size == self.largest_chunk_size:
            self.redis_client.zadd(self.access_times_sorted_sets[size], {str(offset): time.time()})
        return self.redis_client.hget(str(size), str(offset))

    def put(self, offset: int, value: str):
        current_size = 0
        for chunk_size in self.chunk_sizes:
            current_size += chunk_size * self.redis_client.hlen(str(chunk_size))
        size_of_current_chunk = len(value)
        if current_size + size_of_current_chunk > self.max_size:
            recent_large_chunk = \
                self.redis_client.zrevrange(self.access_times_sorted_sets[self.largest_chunk_size], 0, 0,
                                            withscores=True)[0]
            seconds_since_recent_large_chunk = time.time() - recent_large_chunk[1]
            if size_of_current_chunk != self.largest_chunk_size and seconds_since_recent_large_chunk > self.duration_of_largest_chunks:
                size_to_remove = self.largest_chunk_size
                offset_to_remove = recent_large_chunk[0]
            else:
                size_to_remove = size_of_current_chunk
                offset_to_remove = self.redis_client.zrange(self.access_times_sorted_sets[size_of_current_chunk], 0, 0)[
                    0]
            print(
                f"deleting older chunk - size: {size_to_remove}, seconds since recent large chunk: {seconds_since_recent_large_chunk}")
            self.delete(offset_to_remove, size_to_remove)
        self.redis_client.zadd(self.access_times_sorted_sets[size_of_current_chunk], {str(offset): time.time()})
        return self.redis_client.hset(str(size_of_current_chunk), str(offset), value)

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
                if not len(result) == LARGE_REQUEST_SIZE:
                    self.put(offset, result)
                    large_chunk_from_requested_offset = func(offset, LARGE_REQUEST_SIZE)
                    self.put(offset, large_chunk_from_requested_offset)
                return result

        return func_wrapper
