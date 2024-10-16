# Merkle Tree Refactor

* File Chunk Deduplication
  * Contribute back to Polars the fix for 
    * /Users/gregschoeninger/.cargo/registry/src/index.crates.io-6f17d22bba15001f/polars-io-0.41.2/src/mmap.rs
  * Biggest question is if we can reconstruct and render the large data frames efficiently...?
    * yes.
    * can update the download api to use ChunkReader
  * Can optionally "unpack" to full file or index on specific commits
    * Always use chunks
    * This helps us write to the merkle tree in parallel on upload
    * If unpacking is turned on in branch, we unpack chunks into full files or write to duckdb or s3 or w/e
  * File Chunks
    * Can get rid of FileChunkNode type since it's just a file we store on disk now
  * TODO L8R - Can you smartly chunk parquet files on the boundaries of columns?
    * For example, I removed a column of a parquet file, and expected there to be many more shared chunks
    * I think there were more generated just because 16kb is an arbitrary boundary


* Make commit object another node in the merkle tree
  * It can just be a different object type
  * Commit
    * id (is just hash)
    * Author
    * Email
    * Parents
    * Timestamp
    * Root (directory node)
* Create a new ref dbs
  * can ignore the rocksdb in the base dir...
  * create new dbs for
  * refs/branches
  * refs/tags

* Storage Backends
  * Local Backend
  * S3 Backend
    * This would be ridiculous # of files if chunking is turned on...