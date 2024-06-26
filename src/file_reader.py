from src.constants import SMALL_REQUEST_SIZE, LARGE_REQUEST_SIZE, CACHE_MAX_SIZE, SECONDS_OF_LARGE_CHUNKS_IN_CACHE, \
    REDIS_HOST, REDIS_PORT
from src.redis_chunks_cache import RedisChunksCache

cache = RedisChunksCache(REDIS_HOST, REDIS_PORT, [SMALL_REQUEST_SIZE, LARGE_REQUEST_SIZE], CACHE_MAX_SIZE,
                         SECONDS_OF_LARGE_CHUNKS_IN_CACHE)


class FileReader:
    """
    A class that reads chunks from a file that represents our db.
    The class uses the cache in order to store the chunks it gets from the file.
    """

    def __init__(self, path: str):
        self.path = path

    @cache
    def get(self, offset: int, size: int) -> bytes:
        with open(self.path, 'rb') as file:
            file.seek(offset)
            return file.read(size)
