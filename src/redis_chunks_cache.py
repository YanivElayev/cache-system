import functools
import time
from typing import Callable, List

from src.cache import Cache
from redis import Redis

from src.constants import SECONDS_OF_LARGE_CHUNKS_IN_CACHE, LARGE_REQUEST_SIZE


class RedisChunksCache(Cache):
    def __init__(self, redis_host: str, redis_port: int, chunk_sizes: List[int], max_size: int,
                 duration_of_largest_chunk_size=SECONDS_OF_LARGE_CHUNKS_IN_CACHE):
        self.redis_client = Redis(redis_host, redis_port, decode_responses=True)
        self.chunk_sizes = chunk_sizes
        self.access_times_sorted_sets = {chunk_size: f"{chunk_size}_access_times" for chunk_size in chunk_sizes}
        self.max_size = max_size
        self.largest_chunk_size = max(chunk_sizes)
        self.duration_of_largest_chunks = duration_of_largest_chunk_size

    def get(self, offset: int, size: int) -> str:
        if not size == self.largest_chunk_size:
            self.redis_client.zadd(self.access_times_sorted_sets[size], {str(offset): time.time()})
        return self.redis_client.hget(str(size), str(offset))

    def put(self, offset: int, value: str) -> int:
        current_size = 0
        for chunk_size in self.chunk_sizes:  # The amount of sizes is predetermined, so we're iterating over a list
            # in a predetermined length. So the complexity here is O(const number) which in our case is O(2) which is
            # still O(1). I chose to put the sizes in a list just for the convenience.
            current_size += chunk_size * self.redis_client.hlen(str(chunk_size))
        size_of_current_chunk = len(value)
        if current_size + size_of_current_chunk > self.max_size:  # The cache is full.
            oldest_large_chunk = \
                self.redis_client.zrange(self.access_times_sorted_sets[self.largest_chunk_size], 0, 0, withscores=True)[
                    0]
            seconds_since_oldest_large_chunk = time.time() - oldest_large_chunk[1]
            if seconds_since_oldest_large_chunk > self.duration_of_largest_chunks:
                size_to_remove = self.largest_chunk_size
                offset_to_remove = oldest_large_chunk[0]
            else:
                size_to_remove = size_of_current_chunk
                offset_to_remove = self.redis_client.zrange(self.access_times_sorted_sets[size_of_current_chunk], 0, 0)[
                    0]
            print(
                f"deleting older chunk - size: {size_to_remove}, offset: {offset_to_remove}, seconds since oldest large chunk: {seconds_since_oldest_large_chunk}")
            self.delete(offset_to_remove, size_to_remove)
        self.redis_client.zadd(self.access_times_sorted_sets[size_of_current_chunk], {str(offset): time.time()})
        return self.redis_client.hset(str(size_of_current_chunk), str(offset), value)

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
