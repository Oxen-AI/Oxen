# Practical Guide: `heed` (Rust LMDB Wrapper)

`heed` (v0.22) is a fully-typed Rust wrapper around LMDB maintained by Meilisearch. It provides zero-copy reads via memory-mapped I/O and typed key/value databases.

**Cargo.toml:**
```toml
[dependencies]
heed = { version = "0.22", features = ["serde"] }
serde = { version = "1", features = ["derive"] }
byteorder = "1"
```

---

## 1. Creating an Environment

The `Env` is the top-level handle for all LMDB operations. One `Env` per directory.

```rust
use std::fs;
use heed::EnvOpenOptions;

fs::create_dir_all("/path/to/db")?;

let env = unsafe {
    EnvOpenOptions::new()
        .map_size(1024 * 1024 * 1024)  // 1 GB max size (must be multiple of OS page size)
        .max_dbs(10)                    // required if using named databases
        .max_readers(126)               // max concurrent read transactions (default 126)
        .open("/path/to/db")?
};
```

`open()` is `unsafe` because LMDB requires callers to uphold invariants: no network filesystems, no long-lived transactions, and crash-safe lock file handling.

`Env` is `Clone + Send + Sync` -- share it freely across threads.

### Key configuration methods

| Method | Description |
|--------|-------------|
| `map_size(usize)` | Max size the DB can grow to. Does not auto-grow. |
| `max_dbs(u32)` | **Must** set if using named databases. |
| `max_readers(u32)` | Max concurrent reader slots. Default 126. |
| `read_txn_without_tls()` | Makes `RoTxn` `Send` (movable across threads). Slightly slower. |

---

## 2. Creating / Opening Databases

A `Database<KC, DC>` is typed by key codec (`KC`) and value codec (`DC`).

```rust
use heed::types::*;
use heed::Database;

let mut wtxn = env.write_txn()?;

// Named database: string keys, raw byte values
let db: Database<Str, Bytes> = env.create_database(&mut wtxn, Some("my-db"))?;

// Unnamed database (pass None) -- only one per environment
let db: Database<Str, Bytes> = env.create_database(&mut wtxn, None)?;

wtxn.commit()?;
```

`create_database` creates the database if it doesn't exist, or opens it if it does. Requires a write transaction.

To open an existing database without creating:
```rust
let rtxn = env.read_txn()?;
let db: Option<Database<Str, Bytes>> = env.open_database(&rtxn, Some("my-db"))?;
```

---

## 3. Codec Types (`heed::types`)

### Primitives (use big-endian for keys so byte ordering matches numeric ordering)

```rust
use heed::byteorder::BE;
use heed::types::*;

type BEU64 = U64<BE>;    // u64, big-endian
type BEU128 = U128<BE>;  // u128, big-endian
type BEI64 = I64<BE>;    // i64, big-endian
```

### Strings and bytes

| Type | Rust type | Description |
|------|-----------|-------------|
| `Str` | `&str` | UTF-8 string |
| `Bytes` | `&[u8]` | Raw bytes, no transformation |
| `Unit` | `()` | Zero bytes. For set-like databases (keys only). |

### Serde types (requires `features = ["serde"]`)

| Type | Format | Description |
|------|--------|-------------|
| `SerdeBincode<T>` | bincode | Compact binary. Fast. |
| `SerdeJson<T>` | JSON | Human-readable. Larger. |

### Utility types

| Type | Description |
|------|-------------|
| `DecodeIgnore` | Skips decoding entirely. Use when you only need keys. |
| `LazyDecode<C>` | Defers decoding. Returns `Lazy` with `.decode()` method. |

---

## 4. Transactions

### Read-only (`RoTxn`)

```rust
let rtxn = env.read_txn()?;
let val = db.get(&rtxn, "key")?;
// rtxn auto-drops, or call rtxn.commit() to release reader slot explicitly
```

- Many can coexist concurrently.
- Represents a **snapshot** at creation time.
- **Keep short-lived!** Active read transactions prevent LMDB from reusing freed pages.

### Read-write (`RwTxn`)

```rust
let mut wtxn = env.write_txn()?;
db.put(&mut wtxn, "key", &value)?;
wtxn.commit()?;
```

- **Only one at a time.** Other threads block until it commits or aborts.
- Dropped without `commit()` = automatic abort (changes discarded).
- Derefs to `RoTxn`, so you can read through it too.

### Nested transactions

```rust
let mut wtxn = env.write_txn()?;
db.put(&mut wtxn, "a", &[1][..])?;

{
    let mut nested = env.nested_write_txn(&mut wtxn)?;
    db.put(&mut nested, "b", &[2][..])?;
    nested.abort();  // "b" discarded
}

{
    let mut nested = env.nested_write_txn(&mut wtxn)?;
    db.put(&mut nested, "c", &[3][..])?;
    nested.commit()?;  // "c" visible in parent
}

wtxn.commit()?;  // "a" and "c" persisted. "b" was not.
```

---

## 5. CRUD Operations

### Put (insert or update)

```rust
let mut wtxn = env.write_txn()?;
db.put(&mut wtxn, "key", &[1, 2, 3][..])?;
wtxn.commit()?;
```

### Get

```rust
let rtxn = env.read_txn()?;
let value: Option<&[u8]> = db.get(&rtxn, "key")?;
```

### Delete

```rust
let mut wtxn = env.write_txn()?;
let deleted: bool = db.delete(&mut wtxn, "key")?;
wtxn.commit()?;
```

### Delete range

```rust
let mut wtxn = env.write_txn()?;
let count: usize = db.delete_range(&mut wtxn, &(35..=42))?;
wtxn.commit()?;
```

### Clear all entries

```rust
let mut wtxn = env.write_txn()?;
db.clear(&mut wtxn)?;
wtxn.commit()?;
```

### First / Last / Length

```rust
let rtxn = env.read_txn()?;
let first: Option<(&str, &[u8])> = db.first(&rtxn)?;
let last: Option<(&str, &[u8])> = db.last(&rtxn)?;
let count: u64 = db.len(&rtxn)?;
let empty: bool = db.is_empty(&rtxn)?;
```

### Get-or-put (insert if absent)

```rust
let mut wtxn = env.write_txn()?;
// Returns None if inserted, Some(existing) if key existed
let result = db.get_or_put(&mut wtxn, "key", &[1, 2][..])?;
wtxn.commit()?;
```

### Bulk append (fast loading with pre-sorted keys)

```rust
use heed::PutFlags;

let mut wtxn = env.write_txn()?;
// Keys MUST be in ascending sorted order -- very fast, skips B-tree rebalancing
db.put_with_flags(&mut wtxn, PutFlags::APPEND, "aaa", &1)?;
db.put_with_flags(&mut wtxn, PutFlags::APPEND, "bbb", &2)?;
db.put_with_flags(&mut wtxn, PutFlags::APPEND, "ccc", &3)?;
wtxn.commit()?;
```

---

## 6. Iteration

### Forward

```rust
let rtxn = env.read_txn()?;
for result in db.iter(&rtxn)? {
    let (key, value) = result?;
    println!("{key}: {value:?}");
}
```

### Reverse

```rust
for result in db.rev_iter(&rtxn)? {
    let (key, value) = result?;
}
```

### Range

```rust
for result in db.range(&rtxn, &("apple"..="cherry"))? {
    let (key, value) = result?;
}
```

### Prefix (requires `LexicographicComparator` on key type -- `Str` and `Bytes` satisfy this)

```rust
for result in db.prefix_iter(&rtxn, "user:")? {
    let (key, value) = result?;
    // matches "user:1", "user:alice", etc.
}
```

### Mutable iteration (delete during iteration)

```rust
let mut wtxn = env.write_txn()?;
let mut iter = db.iter_mut(&mut wtxn)?;
while let Some(result) = iter.next() {
    let (key, _value) = result?;
    if key.starts_with("temp:") {
        unsafe { iter.del_current()? };
    }
}
drop(iter);
wtxn.commit()?;
```

---

## 7. Serde Example

```rust
use heed::{Database, EnvOpenOptions};
use heed::types::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
struct User {
    name: String,
    age: u32,
}

let mut wtxn = env.write_txn()?;
let db: Database<Str, SerdeBincode<User>> =
    env.create_database(&mut wtxn, Some("users"))?;

db.put(&mut wtxn, "alice", &User { name: "Alice".into(), age: 30 })?;
db.put(&mut wtxn, "bob", &User { name: "Bob".into(), age: 25 })?;
wtxn.commit()?;

let rtxn = env.read_txn()?;
let alice: Option<User> = db.get(&rtxn, "alice")?;
assert_eq!(alice.unwrap().age, 30);
```

---

## 8. u128 Keys (relevant for MerkleHash)

```rust
use heed::byteorder::LE;
use heed::types::*;

type LEU128 = U128<LE>;  // little-endian u128

let mut wtxn = env.write_txn()?;
let db: Database<LEU128, Bytes> = env.create_database(&mut wtxn, Some("nodes"))?;

let hash: u128 = 0xDEADBEEF_CAFEBABE_12345678_9ABCDEF0;
db.put(&mut wtxn, &hash, &[1, 2, 3][..])?;
wtxn.commit()?;

let rtxn = env.read_txn()?;
let data: Option<&[u8]> = db.get(&rtxn, &hash)?;
```

**Note:** Use big-endian (`BE`) if you need LMDB's default byte comparison to match numeric ordering (e.g., for range queries on integer keys). Use little-endian (`LE`) if you only do exact lookups and want to match the in-memory representation.

---

## 9. Environment Lifecycle and Teardown

### Closing

`Env` is reference-counted via `Clone`. The LMDB environment closes when all clones are dropped. To explicitly wait for closure:

```rust
let closing_event = env.prepare_for_closing();
drop(env);  // drop any remaining clones
closing_event.wait();  // blocks until fully closed
```

### Hot backup

```rust
use heed::CompactionOption;

// With compaction (reclaims free pages, smaller file)
env.copy_to_path("backup.mdb", CompactionOption::Enabled)?;

// Without compaction (faster, preserves free pages)
env.copy_to_path("backup.mdb", CompactionOption::Disabled)?;
```

### Force sync to disk

```rust
env.force_sync()?;
```

### Clear stale readers (crashed processes leave locked reader slots)

```rust
let cleared = env.clear_stale_readers()?;
```

### Resize (when MapFull)

LMDB does not auto-grow. If you hit `MapFull`, you must close and reopen:

```rust
use heed::MdbError;

match db.put(&mut wtxn, "key", &value) {
    Err(heed::Error::Mdb(MdbError::MapFull)) => {
        wtxn.abort();
        let closing = env.prepare_for_closing();
        closing.wait();
        // Reopen with larger map_size
        let env = unsafe {
            EnvOpenOptions::new()
                .map_size(2 * 1024 * 1024 * 1024)
                .max_dbs(10)
                .open("/path/to/db")?
        };
    }
    other => { other?; }
}
```

---

## 10. Important Constraints

| Constraint | Detail |
|------------|--------|
| **Single writer** | Only one `RwTxn` at a time across all threads/processes. Others block. |
| **Max key size** | 511 bytes (LMDB compile-time constant). Check with `env.max_key_size()`. |
| **Map size is fixed** | Set upfront. Does not auto-grow. Hit `MapFull` = close + reopen larger. |
| **Short-lived reads** | Active `RoTxn` prevents page reuse, growing the DB file. |
| **No network FS** | LMDB uses mmap + file locks. NFS/CIFS will corrupt. |
| **macOS limit** | ~10 concurrent transactions per process (POSIX semaphore limit). |
| **Named DBs** | Must call `max_dbs()` in `EnvOpenOptions`. Each named DB uses the unnamed DB internally. |
| **TLS mode (default)** | One `RoTxn` per thread. Use `read_txn_without_tls()` for `Send` read transactions. |

---

## 11. Error Types

```rust
use heed::{Error, MdbError};

match result {
    Err(Error::Mdb(MdbError::MapFull)) => { /* need to resize */ }
    Err(Error::Mdb(MdbError::KeyExist)) => { /* used NO_OVERWRITE and key exists */ }
    Err(Error::Mdb(MdbError::NotFound)) => { /* key not found */ }
    Err(Error::Mdb(MdbError::ReadersFull)) => { /* too many concurrent readers */ }
    Err(Error::Mdb(MdbError::DbsFull)) => { /* too many named databases */ }
    Err(Error::Encoding(e)) => { /* serialization failed */ }
    Err(Error::Decoding(e)) => { /* deserialization failed */ }
    Err(Error::Io(e)) => { /* I/O error */ }
    _ => {}
}
```
