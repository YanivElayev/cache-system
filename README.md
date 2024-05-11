
# Cache Manager 

Based on the given read patterns, I developed a cache mechanism based on Redis. 
The cache wraps the file manager, which wraps our db. 

### Chunks' Lifecycle in the Cache 
Each chunk that read from the db will be inserted to the cache in the following order:

* Each 8 KB chunk is saved. From the same offset, a 64 KB chunk is saved for 2 seconds.
* Each request to access an 8 KB chunk will update its last access time in the cache.
* A request to access a 64 KB chunk will not update its last access time since 64 KB chunks have 
expiration date which is 2 seconds after their insertion time.
* If the cache is full and there is an 8 KB chunk to insert, the cache will try to remove the oldest 64 KB
chunk if it is expired. If not, the cache manager will remove the *least recently accessed* 8 KB chunk.
* If the cache is full and there is a 64 KB chunk to insert, the cache will remove the *oldest* 64 KB 
chunk.