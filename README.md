
# Cache Mechanism 

Based on the given read patterns, I developed a cache mechanism based on Redis. 
The cache provides a decorator that wraps the method that gets the chunks from the db and saves 
the chunks in Redis alongside their access times.

### Chunks' Lifecycle in the Cache 
Each chunk that read from the db will be inserted to the cache in the following order:

* Each 8 KB chunk that was read from the db is saved in the cache. From the same offset, a 64 KB chunk is saved for 2 seconds.
* Each request to access an 8 KB chunk will update its last access time in the cache.
* A request to access a 64 KB chunk will not update its last access time since 64 KB chunks have 
expiration date which is 2 seconds after their insertion time.
* If the cache is full and there is an 8 KB chunk to insert, the cache will remove the oldest 64 KB
chunk if it is expired. If not, the cache manager will remove the *least recently accessed* 8 KB chunk.
* If the cache is full and there is a 64 KB chunk to insert, the cache will remove the *oldest* 64 KB 
chunk.

The 64 KB chunks are considered temporary chunks in the cache because it is common for a request to fetch
64 KB just 2 seconds after there was a request to fetch 8 KB from the same offset, but for most of the requests, a 64KB
read-request does not follow the repeated read-request patterns. 
The 8 KB chunks are saved in an LRU (least recently used) cache strategy, which means when the cache is
full, the chunk that was least recently accessed will be removed in order to make room for the new chunk.
But, the first thing the cache will do when the cache is full is to remove the oldest 64 KB chunk if
the chunk is expired - which is 2 seconds after its insertion time. If there has not been 2 seconds since the
insertion time of the oldest 64 KB chunk, it means the oldest 64 KB chunk is still might be accessed, so we
have to remove the least recently used 8 KB chunk.
When we save a 64 KB chunk, the oldest 64 KB chunk will be removed from the cache, 
no matter how many seconds pass from its insertion time, which means the cache strategy that we implemented 
for 64 KB chunks is FIFO.

### Redis Sets Structure

The chunks are saved in Hashes. Each hash object represents chunk size. Each field represents offset.
The value of each field is the chunk.

![8192 hset](https://github.com/YanivElayev/cache-system/assets/40890285/172fd3b8-84b6-439f-a046-7c5509be90c9)

In addition to the hashes, There is a sorted set to each chunk size. The sorted set of 8 KB chunks sorts offsets
by their access times. Each time there is a read request from an offset to an 8 KB chunk, its access 
time is updated and the chunk will be in the head of the set. 
The sorted set of 64 KB chunks sorts offsets by their insertion time since the 64 KB chunks have predetermined 
expiration time which is 2 seconds after their insertion time.

![redis sorted set 8192](https://github.com/YanivElayev/cache-system/assets/40890285/c00f4a69-e9ee-4e82-a880-4900e3c714af)

![redis sorted set 65536](https://github.com/YanivElayev/cache-system/assets/40890285/b62e0c99-7039-41e5-b0e9-686b302236ab)



