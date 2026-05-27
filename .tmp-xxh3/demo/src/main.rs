use xxhash_rust::xxh3::{Xxh3, xxh3_128};

fn main() {
    // 1. The byte value 0 (a single 0x00 byte).
    let single_zero = xxh3_128(&[0u8]);
    println!("xxh3_128(&[0u8])      = {single_zero:#034x}  ({single_zero})");

    // 2. Empty input -- "hashing nothing".
    let empty = xxh3_128(&[]);
    println!("xxh3_128(&[])         = {empty:#034x}  ({empty})");

    // 3. Empty input via the streaming API with zero updates.
    let h = Xxh3::new();
    let empty_stream = h.digest128();
    println!("Xxh3::new().digest128 = {empty_stream:#034x}  ({empty_stream})");

    // 4. The integer 0 as little-endian u128 bytes (16 zero bytes).
    let zero_u128 = xxh3_128(&0u128.to_le_bytes());
    println!("xxh3_128(0u128 le)    = {zero_u128:#034x}  ({zero_u128})");
}
