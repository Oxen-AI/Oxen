use xxhash_rust::xxh3::{xxh3_128};


pub fn hash_buffer_128bit(buffer: &[u8]) -> u128 {
    xxh3_128(buffer)
}