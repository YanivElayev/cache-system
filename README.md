
# Cache Manager 

Based on the given read patterns, I developed a cache mechanism based on Redis. 
The cache wraps the file manager, which wraps our db. 

### Insert Method
Each chunk that read from the db will be inserted to the cache in the following order:

* Each 8 KB chunk is saved. From the same offset, a 64 KB chunk is saved for 2 seconds.
* If the cache is full and there is an 8 KB chunk to insert, the cache manager will try to remove the oldest 64 KB
chunk if it is expired. If not, the cache manager will remove the least recently accessed 8 KB chunk.
* If the cache is full and there is a 64 KB chunk to insert, the cache will remove the oldest 64 KB 
chunk.

### Access Method
