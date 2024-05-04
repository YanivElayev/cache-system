import functools
import pickle
import time
from typing import Callable, List

from src.cache import Cache
from redis import Redis

from src.constants import SECONDS_OF_LARGE_CHUNKS_IN_CACHE, LARGE_REQUEST_SIZE


class RedisChunksCache(Cache):
    def __init__(self, redis_host: str, redis_port: int, chunk_sizes: List[int], max_size: int,
                 seconds_of_large_chunks_in_cache=SECONDS_OF_LARGE_CHUNKS_IN_CACHE):
        self.redis_client = Redis(redis_host, redis_port, decode_responses=True)
        self.chunk_sizes = chunk_sizes
        self.access_times_sorted_sets = {chunk_size: f"{chunk_size}_access_times" for chunk_size in chunk_sizes}
        self.max_size = max_size
        self.largest_chunk_size = max(chunk_sizes)
        self.seconds_of_large_chunks_in_cache = seconds_of_large_chunks_in_cache

    def get(self, offset: int, size: int) -> str:
        if not size == self.largest_chunk_size:
            self.redis_client.zadd(self.access_times_sorted_sets[size], {str(offset): time.time()})
        return self.redis_client.hget(str(size), str(offset))

    def put(self, offset: int, value: bytes) -> int | None:
        current_size = 0
        for chunk_size in self.chunk_sizes:  # Still O(1) since the sizes are predetermined. I decided to put the sizes
            # in list just for the convenience.
            current_size += chunk_size * self.redis_client.hlen(str(chunk_size))
        size_of_current_chunk = len(value)
        if current_size + size_of_current_chunk > self.max_size:  # The cache is full
            recent_large_chunk = \
                self.redis_client.zrange(self.access_times_sorted_sets[self.largest_chunk_size], 0, 0, withscores=True)
            seconds_since_oldest_large_chunk = 0
            if size_of_current_chunk == self.largest_chunk_size and len(recent_large_chunk) == 0:  # If we don't have
                # large chunks to remove so we don't save the new one. We don't remove hot block of 8kb for a
                # temporary 64kb block that will be removed after 2 seconds.
                print("cannot insert large chunk")
                return
            elif len(recent_large_chunk) > 0:
                seconds_since_oldest_large_chunk = time.time() - recent_large_chunk[0][1]
            if size_of_current_chunk != self.largest_chunk_size and seconds_since_oldest_large_chunk > self.seconds_of_large_chunks_in_cache:
                size_to_remove = self.largest_chunk_size
                offset_to_remove = recent_large_chunk[0][0]
            else:
                size_to_remove = size_of_current_chunk
                offset_to_remove = self.redis_client.zrange(self.access_times_sorted_sets[size_of_current_chunk], 0, 0)[
                    0]
            print(
                f"deleting older chunk - size: {size_to_remove}, offset: {offset_to_remove}, seconds since least recent large chunk: {seconds_since_oldest_large_chunk}")
            self.delete(offset_to_remove, size_to_remove)
        self.redis_client.zadd(self.access_times_sorted_sets[size_of_current_chunk], {str(offset): time.time()})
        return self.redis_client.hset(str(size_of_current_chunk), str(offset), str(value))

    def delete(self, offset: int, size: int) -> int:
        self.redis_client.zrem(self.access_times_sorted_sets[size], offset)
        return self.redis_client.hdel(str(size), str(offset))

    def __call__(self, func: Callable) -> Callable:
        @functools.wraps(func)
        def func_wrapper(*args):
            offset, size = args[1], args[2]
            value = self.get(offset, size)
            if value:
                print("received from cache essekititttttt")
                return value
            else:
                print("received from function's execution")
                result = func(*args)
                if not len(result) == LARGE_REQUEST_SIZE:
                    self.put(offset, result)
                    large_chunk_from_requested_offset = func(args[0], offset, LARGE_REQUEST_SIZE)
                    self.put(offset, large_chunk_from_requested_offset)
                return result

        return func_wrapper
