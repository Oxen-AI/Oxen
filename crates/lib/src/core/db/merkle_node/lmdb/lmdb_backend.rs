use std::path::{Path, PathBuf};

use bytesize::ByteSize;
use heed::byteorder::LE;
use heed::types::{Bytes, DecodeIgnore, U128};
use heed::{AnyTls, Database, Env, EnvOpenOptions, WithTls};

use crate::constants::OXEN_HIDDEN_DIR;
use crate::core::db::merkle_node::lmdb::LmdbError;
use crate::core::db::merkle_node::lmdb::hash_content_name::HashCN;
use crate::core::db::merkle_node::lmdb::value_structs::{LmdbDupes, LmdbLink, LmdbNode};
use crate::model::{MerkleHash, MerkleTreeNodeType};
use crate::util;

/// Keys are merkle hashes, which are `u128` values.
/// MUST USE LITTLE ENDIAN (LE) so the byte layout matches
/// `ContentHash::as_bytes` / `LocationHash::as_bytes`.
pub(super) type KeyLmdb = U128<LE>;

/// Values are serialized as bytes. Access requires deserialization into structs.
pub(super) type ValueLmdb = Bytes;

/// LMDB table name: storing merkle tree node byte
const DB_NODE: &str = "merkle_node_store";
const DB_DUPES: &str = "merkle_node_dupes";
/// LMDB table name: storing parent & child relationships per-node
const DB_LINKS: &str = "merkle_links";

/// Merkle tree node storage that is backed by LMDB.
///
/// NOTE: DO NOT USE ON A VIRTUAL FILE SYSTEM !!
///       DO NOT OPEN AN MORE THAN ONE [`LmdbBackend`] PER REPOSITORY IN A PROCESS.
pub struct LmdbBackend {
    /// The filesystem location of the local repository.
    pub(super) repo_root: PathBuf,
    /// The LMDB environment that contains the [`Database`] fields.
    pub(super) lmdb_env: Env<WithTls>,
    /// All LMDB tables in use by the backend.
    pub(super) tables: LmdbTables,
}

pub(in crate::core::db::merkle_node::lmdb) struct LmdbTables {
    // Stores every kind of merkle tree node: any concrete [`MerkleTreeNode`].
    // This includes files nodes! Note that there is no other data but the Merkle
    // tree node type (u8) and the msgpack-serialized bytes of the actual node.
    //
    // These values deserialize into a [`LmdbNode`]. The  zerocopy view on these
    // values is the [`LmdbNodeRef`].
    //
    // In the LMDB environment, this has name of the [DB_NODES] constant.
    /// Maps HashNC to the actual merkle tree node's bytes.
    ///
    /// -----------------------------------------------------------------------
    /// HashNC -> Node
    /// u128   -> variable-length bytes
    /// HashNC = XXH3([MerkleHash(content), XXH3(name)])
    ///        = XXH3([u128,                u128])
    pub merkle_node_store: Database<KeyLmdb, ValueLmdb>,

    /// Maps the merkle hash of the content to a vector of duplicate filenames.
    ///
    /// [`MerkleHash`] values are the same for two files with identicial content.
    /// We, however, need to separately track each uniquely named file in the tree.
    /// This mapping lets us track duplicates directly.
    ///
    /// -----------------------------------------------------------------------
    /// MerkleHash(content) -> Vec<XXH3(name)>
    /// u128                -> Vec<u128>
    pub merkle_node_dupes: Database<KeyLmdb, ValueLmdb>,

    /// Stores the parent and children connections for each Merkle tree node.
    ///
    /// These values deserialize into a [`LmdbLink`]. The zerocopy view on these
    /// values is the [`LmdbLinkRef`].
    ///
    /// In the LMDB environment, this has name of the [DB_LINKS] constant.
    /// -----------------------------------------------------------------------
    /// MerkleHash(content) -> Vec<(MerkleHash(content), XXH3(name))
    /// u128                -> Vec<(u128,                u128)>
    pub merkle_links: Database<KeyLmdb, ValueLmdb>,
}

// on every platform.
const WORST_CASE_PAGE_SIZE_BYTES: u64 = 64 * 1024;
const _: () = assert!(
    ByteSize::gib(1)
        .as_u64()
        .is_multiple_of(WORST_CASE_PAGE_SIZE_BYTES),
    "DEFAULT_LMDB_MMAP_SIZE must be aligned to the worst-case OS page size \
     (64 KiB); change it to a multiple of 65536 if you adjust the value."
);

// 1 GiB ceiling — large enough for typical Merkle trees,
// small enough to keep sparse-file disk usage in check. Can
// be revisited if/when we have repos that exceed it.
//
// LMDB requires `map_size` to be a multiple of the OS page size. The OS
// page size is a runtime value — there's no stable Rust API exposing it
// in `const` — so we can't dynamically pick a "next multiple of page
// size" here. Instead we hardcode 1 GiB (= 2^30 bytes), and `const-assert`
// it's a multiple of the worst-case page size any real OS uses (64 KiB,
// = 2^16, on PPC64; macOS arm64 is 16 KiB; x86_64 Linux/Windows is 4 KiB).
// 2^30 is divisible by 2^16, so it's also divisible by 2^14 and 2^12,
// which covers every real platform.
//
// Must use `gib` (binary, 2^30 = 1_073_741_824) and not `gb` (decimal
// 10^9 = 1_000_000_000): 10^9 is not a power of two and is not a
// multiple of any common page size, so it would fail LMDB validation
pub(crate) const DEFAULT_LMDB_MMAP_SIZE: ByteSize = ByteSize::gib(1);

/// Configures LMDB options for the [`LmdbBackend`] with the given mmap file size.
///
/// If the file size is not a multiple of the OS's page size, then the value is
/// rounded up to the nearest multiple of the page size. LMDB requires the mmap file
/// to be a multiple of the page size.
///
/// If a size greater than the total addressible memory space is provided, then this
/// function will return an [`LmdbError::InitMmap`] error.
pub(crate) fn lmdb_backend_options(size: ByteSize) -> Result<EnvOpenOptions, LmdbError> {
    // The only reason to reject a requested size outright is that it doesn't
    // fit in this platform's `usize` (the type LMDB's `map_size` takes). On
    // 64-bit targets this is effectively unreachable; on 32-bit it caps at
    // ~4 GiB. If the size *does* fit but isn't a multiple of the current OS
    // page size, round up to the next multiple — LMDB requires page-aligned
    // map sizes, and silently accepting an unaligned value would surface as a
    // confusing `EINVAL` from `mdb_env_set_mapsize`.
    let requested = size.as_u64();
    let map_size = usize::try_from(requested).map_err(|_| LmdbError::InitMmap(size))?;

    let page_size = page_size::get();
    let aligned = if map_size.is_multiple_of(page_size) {
        map_size
    } else {
        let rounded = map_size.next_multiple_of(page_size);
        log::warn!(
            "LMDB map_size {map_size} is not a multiple of the OS page size {page_size}; \
             rounding up to {rounded}",
        );
        rounded
    };

    let mut options = EnvOpenOptions::new();
    options.map_size(aligned);
    // for LmdbBackend's 3 `Database` fields
    options.max_dbs(3);
    Ok(options)
}

impl LmdbBackend {
    /// Constructs a new LMDB environment and uses it as a Merkle tree node store.
    ///
    /// Saves the LMDB file to under the `.oxen/` directory of the supplied repository root
    /// (`repo_root`). LMDB internally mmaps this file. This [`LmdbBackend`] struct holds on
    /// to a mmap handle. This handle is dropped when this struct is dropped.
    ///
    /// **NOT THE PUBLIC ENTRY POINT.** All in-crate construction must go through
    /// [`crate::core::db::merkle_node::lmdb::cache::get_or_open`], which serializes
    /// opens process-wide and shares one `Arc<LmdbBackend>` across overlapping callers.
    ///
    /// **SAFETY**: LMDB rejects opening the same env directory twice in one process. The
    ///             [`lmdb::cache::get_or_open`] function caches weak references to [`LmdbBackend`]
    ///             instances and their LMDB environment.
    ///
    ///             If you're using this function, either use unique LMDB locations or drop this
    ///             struct before opening up the LMDB env again.
    pub(crate) fn new(repo_root: PathBuf, options: EnvOpenOptions) -> Result<Self, LmdbError> {
        log::info!("Config for LMDB backend: {options:?}");

        let lmdb_env = {
            let db_location = lmdb_dir_location(&repo_root);
            util::fs::create_dir_all(&db_location).map_err(|e| LmdbError::InitDir(Box::new(e)))?;

            log::info!("Opening LMDB backend at: {}", db_location.display());
            // SAFETY: LMDB uses a memory mapped file. If this file is modified in or out of process,
            //         it can cause undefined behavior. There is nothing else in the Oxen codebase that
            //         modifies this mmap'd file. Nothing else access `db_location` except for this code.
            //         This uses LMDB's own opening code, which does lock the file to check for and
            //         prevent concurrent access. The fact that it is a "hidden" file helps somewhat: it will
            //         be unlikely for someone or something to accidentily stumble upon it and mess things up.
            unsafe { options.open(db_location).map_err(LmdbError::Access)? }
        };

        let mut wtxn = lmdb_env.write_txn().map_err(LmdbError::Access)?;
        let merkle_node_store = lmdb_env
            .create_database::<KeyLmdb, ValueLmdb>(&mut wtxn, Some(DB_NODE))
            .map_err(LmdbError::Write)?;
        // let merkle_node_dupes = lmdb_env
        //     .database_options()
        //     .types::<KeyLmdb, ValueLmdb>()
        //     .flags(heed::DatabaseFlags::DUP_SORT)
        //     .name(DB_DUPES)
        //     .create(&mut wtxn)
        //     .map_err(LmdbError::Write)?;
        let merkle_node_dupes = lmdb_env
            .create_database::<KeyLmdb, ValueLmdb>(&mut wtxn, Some(DB_DUPES))
            .map_err(LmdbError::Write)?;
        let merkle_links = lmdb_env
            .create_database::<KeyLmdb, ValueLmdb>(&mut wtxn, Some(DB_LINKS))
            .map_err(LmdbError::Write)?;
        wtxn.commit().map_err(LmdbError::Write)?;

        Ok(Self {
            repo_root,
            lmdb_env,
            tables: LmdbTables {
                merkle_node_store,
                merkle_node_dupes,
                merkle_links,
            },
        })
    }

    /// Private helper for creating a new LMDB write transaction.
    /// Returns [`LmdbError::Access`] on error.
    pub(in crate::core::db::merkle_node::lmdb) fn write_txn<'a>(
        lmdb_env: &'a heed::Env<heed::WithTls>,
    ) -> Result<heed::RwTxn<'a>, LmdbError> {
        lmdb_env.write_txn().map_err(LmdbError::Access)
    }

    /// Private helper for creating a new LMDB read transaction.
    /// Returns [`LmdbError::Access`] on error.
    pub(in crate::core::db::merkle_node::lmdb) fn read_txn<'a>(
        &'a self,
    ) -> Result<heed::RoTxn<'a, heed::WithTls>, LmdbError> {
        self.lmdb_env.read_txn().map_err(LmdbError::Access)
    }

    /// Retrieve the raw stored value bytes, or `None` if no entry exists for the key.
    ///
    /// The returned slice borrows directly into the LMDB read transaction's mmap
    /// region — no copy, no parse. Callers wrap it in [`super::value_structs::LmdbNodeRef`]
    /// or [`super::value_structs::LmdbLinkRef`] to get a typed zero-copy view.
    ///
    /// Generic over the `TLS` marker so the helper works with whatever thread local storage
    /// choice the [`Env`] was opened with. The `Deref` bound is what lets `db.get(rtxn, ..)`
    /// accept `&heed::RoTxn<'a, T>` — heed's `Database::get` requires `&RoTxn<'_, AnyTls>`
    /// and both `WithTls` and `WithoutTls` deref to it.
    ///
    /// Returns an [`LmdbError::Retrieve`] on error.
    #[inline(always)]
    pub(super) fn retrieve_bytes<'a, TLS>(
        rtxn: &'a heed::RoTxn<'a, TLS>,
        db: &Database<KeyLmdb, ValueLmdb>,
        key: &u128,
    ) -> Result<Option<&'a [u8]>, LmdbError>
    where
        heed::RoTxn<'a, TLS>: std::ops::Deref<Target = heed::RoTxn<'a, AnyTls>>,
    {
        db.get(rtxn, key).map_err(LmdbError::Retrieve)
    }

    /// True if the given key exists in the database. False otherwise.
    /// Does no decoding or other handling of the stored value.
    ///
    /// Generic over the `TLS` marker — see [`Self::retrieve`] for the rationale on the bound.
    ///
    /// Returns an [`LmdbError::Retrieve`] on error.
    #[inline(always)]
    pub(super) fn key_present<'a, TLS>(
        rtxn: &heed::RoTxn<'a, TLS>,
        db: &Database<KeyLmdb, ValueLmdb>,
        key: &u128,
    ) -> Result<bool, LmdbError>
    where
        heed::RoTxn<'a, TLS>: std::ops::Deref<Target = heed::RoTxn<'a, AnyTls>>,
    {
        Ok(db
            .remap_data_type::<DecodeIgnore>()
            .get(rtxn, key)
            .map_err(LmdbError::Retrieve)?
            .is_some())
    }

    /// Serialize the value and put it into the database with the given key.
    /// Is an error if serialization fails (heed::Error::Encoding).
    ///
    /// Returns an [`LmdbError::Write`] on error.
    #[inline(always)]
    pub(super) fn put_serialized<'a, L, S: Fn(L) -> Vec<u8>>(
        wtxn: &mut heed::RwTxn<'a>,
        db: &Database<KeyLmdb, ValueLmdb>,
        key: &u128,
        serialize: S,
        value: L,
    ) -> Result<(), LmdbError> {
        let as_bytes = serialize(value);
        db.put(wtxn, key, &as_bytes).map_err(LmdbError::Write)
    }

    /// Checks if a node with the given `hash` exists in the store.
    ///
    /// Unlike [`MerkleReader::exists`], this method will return `Ok(true)` for file nodes
    /// if the file exists in the tree.
    pub fn full_exists(&self, hash: &HashCN) -> Result<bool, LmdbError> {
        let exists = Self::key_present(
            &self.read_txn()?,
            &self.tables.merkle_node_store,
            hash.as_u128(),
        )?;
        Ok(exists)
    }

    /// Retrieves the node with the given `hash` from the store. `None` means no such node exists.
    ///
    /// Unlike [`MerkleReader::get_node`], this method will return `Ok(Some(.))` for file nodes
    /// if the file exists in the tree.
    ///
    /// Internally reads the value bytes directly from LMDB's mmap and decodes via
    /// [`LmdbNode::decode`], which uses the zero-copy [`super::value_structs::LmdbNodeRef`]
    /// path to validate the header before copying out the msgpack tail.
    pub fn full_get_node(&self, hash: &HashCN) -> Result<Option<LmdbNode>, LmdbError> {
        let rtxn = self.read_txn()?;
        let Some(bytes) =
            Self::retrieve_bytes(&rtxn, &self.tables.merkle_node_store, hash.as_u128())?
        else {
            return Ok(None);
        };
        Ok(Some(LmdbNode::decode(bytes)?))
    }

    pub fn get_duplicates(&self, hash: &MerkleHash) -> Result<Option<LmdbDupes>, LmdbError> {
        let rtxn = self.read_txn()?;
        let Some(bytes) =
            Self::retrieve_bytes(&rtxn, &self.tables.merkle_node_dupes, &hash.to_u128())?
        else {
            return Ok(None);
        };
        Ok(Some(LmdbDupes::decode(bytes)?))
    }

    /// Retreives the node's parent link (if present) and its children. Returns Ok(None)
    /// if there is no node for the given hash.
    pub fn get_links(&self, hash: &MerkleHash) -> Result<Option<LmdbLink>, LmdbError> {
        let rtxn = self.read_txn()?;
        let Some(bytes) = Self::retrieve_bytes(&rtxn, &self.tables.merkle_links, &hash.to_u128())?
        else {
            return Ok(None);
        };
        Ok(Some(LmdbLink::decode(bytes)?))
    }

    // /// Flushes LMDB to disk and closes the underlying LMDB environment for this process.
    // pub fn close(self) -> Result<(), heed::Error> {
    //     log::info!("Preparing to close LMDB");
    //     // drop everything but the LMDB env: we prepare that for closing and wait on it instead
    //     let LmdbBackend { repo_root:_ , lmdb_env, merkle_tree_nodes: _, merkle_links: _ } = self;
    //     lmdb_env.force_sync()?;
    //     let signal_event = lmdb_env.prepare_for_closing();
    //     log::info!("Waiting for the LMDB environment to close");
    //     signal_event.wait();
    //     Ok(())
    // }

    /// Only gets a reference to the stored node information in LMDB for non-file/file chunk nodes.
    ///
    /// When packing into the legacy file backend's tar-gz format, we need to make sure that
    /// file and file chunk nodes are only transmitted in the `children` file. The [`FileBackend`]
    /// only transmits commit, directory, and virtual directory Merkle tree nodes as `node` files.
    ///
    /// When packing, we skip over a file/file chunk node when "sending a node" because that's
    /// sending a `node` file. That same file/file chunk node **WILL** be included when sending
    /// the `children` file.
    #[inline(always)]
    pub(super) fn get_stored_non_file_node(
        &self,
        hash: &MerkleHash,
    ) -> Result<Option<LmdbNode>, LmdbError> {
        let rtxn = self.read_txn()?;

        let hash_cns_for_content = {
            let Some(bytes) =
                Self::retrieve_bytes(&rtxn, &self.tables.merkle_node_dupes, &hash.to_u128())?
            else {
                return Ok(None);
            };

            LmdbDupes::decode(bytes)?
        };

        // Don't encode a file or file chunk node here -- it will be handled
        // in its associated dir/vnode's children.
        if hash_cns_for_content.hash_cns.len() != 1 {
            // Only files will potentially have more than one `HashCN` for it.
            return Ok(None);
        }
        let hash_cn = &hash_cns_for_content.hash_cns[0];

        let stored_node = {
            let Some(bytes) =
                Self::retrieve_bytes(&rtxn, &self.tables.merkle_node_store, hash_cn.as_u128())?
            else {
                // we don't actually have this node -- this is an invairant violation, since
                // we have it in the dupes table. But here we can't get anything useful.
                log::error!(
                    "LMDB invariant violation: have content-name hash {} listed for content hash {}, \
                    but main node store table doesn't have this content-name hash!",
                    hash_cn.to_hex_hash(),
                    hash.to_hex_hash(),
                );
                return Ok(None);
            };
            LmdbNode::decode(bytes)?
        };
        if matches!(
            stored_node.kind,
            MerkleTreeNodeType::File | MerkleTreeNodeType::FileChunk
        ) {
            // Don't consider a unique file.
            return Ok(None);
        }
        Ok(Some(stored_node))
    }
}

impl Drop for LmdbBackend {
    fn drop(&mut self) {
        if let Err(e) = self.lmdb_env.force_sync() {
            log::error!("Could not flush LMDB state to disk! Error: {e}");
        }
    }
}

/// The name of the LMDB directory as it exists in the repository's `.oxen/` hidden directory.
/// LMDB's actual physical contents and lock files are stored within this directory.
const OXEN_LMDB_MERKLE_DIR: &str = "lmdb_merkle_tree_store";

/// The complete filepath to the LMDB file for the given repository.
///
/// `pub(crate)` because test modules outside the `lmdb` namespace (e.g.
/// `repositories::tests`, `local_repository::tests`) consult it to assert
/// that the env directory was created on disk. The actual production
/// users are inside `lmdb/` and would only need `pub(super)`.
pub(crate) fn lmdb_dir_location(repo_root: &Path) -> PathBuf {
    repo_root.join(OXEN_HIDDEN_DIR).join(OXEN_LMDB_MERKLE_DIR)
}

/// Only displays the repository root and LMDB environment settings.
impl std::fmt::Debug for LmdbBackend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LmdbBackend")
            .field("repo_root", &self.repo_root)
            .field("lmdb_env", &self.lmdb_env.info())
            .finish()
    }
}
