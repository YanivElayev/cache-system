from src.constants import SMALL_REQUEST_SIZE, LARGE_REQUEST_SIZE, FILE_PATH
from src.file_reader import FileReader


def main():
    db = FileReader(FILE_PATH)
    db.get(100, SMALL_REQUEST_SIZE)  # reads the chunk from the file and store it in the cache.
    # From the same offset, a 64 KB chunk will be stored for 2 seconds.
    db.get(100, SMALL_REQUEST_SIZE)  # reads the chunk from the cache.
    db.get(100, LARGE_REQUEST_SIZE)  # reads the chunk from the cache.
    db.get(90, SMALL_REQUEST_SIZE)  # In case the cache is full, the oldest 64 KB chunk will be removed if there has
    # been 2 seconds since its insertion time. Else, the least recently used 8 KB chunk will be removed from the cache
    # in order to make room for the new chunk.

if __name__ == '__main__':
    main()
