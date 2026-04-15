use std::fmt;
use std::path::PathBuf;

use serde::de::{self, DeserializeSeed, EnumAccess, MapAccess, SeqAccess, VariantAccess, Visitor};
use serde::{Deserialize, Deserializer, Serialize};

use crate::explore::new_path::{AbsolutePath, RelativePath};
use crate::explore::scratch::{Hash, HexHash, Repository};

//
// M e r k l e   T r e e   D a t a b a s e   S t o r e
//

pub trait MerkleMetadataStore: Sized {
    type Error: std::error::Error;

    /// If true, then there is a node in the Merkle tree that has this hash.
    ///
    /// This always means that either `self.node()` xor `self.commit()` will return a
    /// non-`None` value. Note that this is a mutually exclusive relationship: exactly
    /// one of `node` or `commit` is non-`None` if `exists` is `true`.
    fn exists(&self, hash: Hash) -> Result<bool, Self::Error>;

    /// Obtains a reference to the Merkle tree node for the given hash.
    ///
    /// Corresponds to a real file or directory under version control.
    /// None means there is no node with that hash.
    fn node<'a>(&'a self, hash: Hash) -> Result<Option<MerkleTreeL<'a, Self>>, Self::Error>;

    /// Obtains the commit node, which is the root of the Merkle tree.
    ///
    /// Corresponds to the complete state of the repository at a given commit.
    /// None means there is no commit with that hash.
    fn commit(&self, hash: Hash) -> Result<Option<&Root<Self>>, Self::Error>;

    /// The repository for which this trait is managing the Merkle tree.
    fn repository(&self) -> &Repository;

    /// The repository relative path to the file or directory indicated by the given hash.
    /// None means that the hash does not appear in the Merkle tree.
    fn path(&self, hash: Hash) -> Result<Option<RelativePath>, Self::Error> {
        let rel_path = {
            let mut reverse_path = Vec::new();

            let mut current_hash = hash;
            loop {
                let next_node = self.node(current_hash)?;
                if let Some(next) = next_node {
                    reverse_path.push(next.name().to_string());
                    if let Some(parent) = next.parent() {
                        current_hash = parent.load()?.hash();
                    } else {
                        break;
                    }
                } else {
                    // In case the parent is the commit node / root => we check and
                    // treat this as the end of the path.
                    if self.exists(current_hash)? {
                        break;
                    } else {
                        return Ok(None);
                    }
                }
            }

            reverse_path.reverse();
            reverse_path
        };

        if rel_path.is_empty() {
            Ok(None)
        } else {
            let components = rel_path.iter().map(|s| s.to_string()).collect();
            // SAFETY: we know that each component we've collected in `rel_path` is an actual
            //         file or directory name. We also know that this forms a relative path
            //         within the repository.
            Ok(Some(RelativePath::from_parts(components)))
        }
    }
}

pub enum MerkleTreeL<'a, DB: MerkleMetadataStore> {
    Dir {
        hash: Hash,
        name: String,
        parent: Option<LazyNode<'a, DB>>,
        children: Vec<LazyNode<'a, DB>>,
    },
    File {
        // hash is in content (LazyData)
        name: String,
        parent: Option<LazyNode<'a, DB>>,
        content: LazyData<'a, DB>,
    },
}

impl<DB: MerkleMetadataStore> Serialize for MerkleTreeL<'_, DB> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeStructVariant;
        match self {
            MerkleTreeL::Dir {
                hash,
                name,
                parent,
                children,
            } => {
                let mut sv = serializer.serialize_struct_variant("MerkleTreeL", 0, "Dir", 4)?;
                sv.serialize_field("hash", hash)?;
                sv.serialize_field("name", name)?;
                sv.serialize_field("parent", parent)?;
                sv.serialize_field("children", children)?;
                sv.end()
            }
            MerkleTreeL::File {
                name,
                parent,
                content,
            } => {
                let mut sv = serializer.serialize_struct_variant("MerkleTreeL", 1, "File", 3)?;
                sv.serialize_field("name", name)?;
                sv.serialize_field("parent", parent)?;
                sv.serialize_field("content", content)?;
                sv.end()
            }
        }
    }
}

pub struct LazyNode<'a, DB: MerkleMetadataStore> {
    db: &'a DB,
    me: Hash,
}

impl<'a, DB: MerkleMetadataStore> LazyNode<'a, DB> {
    pub fn load(&self) -> Result<MerkleTreeL<'a, DB>, DB::Error> {
        let Some(node) = self.db.node(self.me)? else {
            panic!(
                "[ERROR] merkle tree node stored incorrectly, cannot find node with hash: {}",
                HexHash::from(self.me)
            );
        };
        Ok(node)
    }

    pub fn hash(&self) -> Hash {
        self.me
    }
}

impl<DB: MerkleMetadataStore> Serialize for LazyNode<'_, DB> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeStruct;
        let mut s = serializer.serialize_struct("LazyNode", 1)?;
        s.serialize_field("me", &self.me)?;
        s.end()
    }
}

#[derive(Debug)]
pub struct LazyData<'a, DB: MerkleMetadataStore> {
    db: &'a DB,
    me: Hash,
}

impl<DB: MerkleMetadataStore> Serialize for LazyData<'_, DB> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeStruct;
        let mut s = serializer.serialize_struct("LazyData", 1)?;
        // Serialize a placeholder — here we compute an ID from the object
        s.serialize_field("me", &self.me)?;
        s.end()
    }
}

pub(crate) struct HasDbSeed<'a, DB: MerkleMetadataStore> {
    db: &'a DB,
}

impl<'a, 'de, DB: MerkleMetadataStore> DeserializeSeed<'de> for HasDbSeed<'a, DB> {
    type Value = LazyData<'a, DB>;

    fn deserialize<D: Deserializer<'de>>(self, deserializer: D) -> Result<Self::Value, D::Error> {
        deserializer.deserialize_struct("LazyData", &["db", "me"], LazyDataVisitor { db: self.db })
    }
}

pub(crate) struct LazyDataVisitor<'a, DB: MerkleMetadataStore> {
    db: &'a DB,
}

impl<'a, 'de, DB: MerkleMetadataStore> Visitor<'de> for LazyDataVisitor<'a, DB> {
    type Value = LazyData<'a, DB>;

    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "LazyData with object reference")
    }

    fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Self::Value, A::Error> {
        let me: Hash = seq
            .next_element()?
            .ok_or_else(|| de::Error::invalid_length(0, &self))?;
        Ok(LazyData { db: self.db, me })
    }

    fn visit_map<M: MapAccess<'de>>(self, mut map: M) -> Result<Self::Value, M::Error> {
        let mut me: Option<Hash> = None;

        while let Some(key) = map.next_key::<String>()? {
            match key.as_str() {
                "me" => me = Some(map.next_value()?),
                _ => {
                    let _ = map.next_value::<de::IgnoredAny>()?;
                }
            }
        }

        let me = me.ok_or_else(|| de::Error::missing_field("me"))?;

        Ok(LazyData { db: self.db, me })
    }
}

//
// D e s e r i a l i z a t i o n   S e e d s
//

// -- LazyNode seed --

pub(crate) struct LazyNodeSeed<'a, DB: MerkleMetadataStore> {
    db: &'a DB,
}

impl<'a, 'de, DB: MerkleMetadataStore> DeserializeSeed<'de> for LazyNodeSeed<'a, DB> {
    type Value = LazyNode<'a, DB>;

    fn deserialize<D: Deserializer<'de>>(self, deserializer: D) -> Result<Self::Value, D::Error> {
        deserializer.deserialize_struct("LazyNode", &["me"], LazyNodeVisitor { db: self.db })
    }
}

pub(crate) struct LazyNodeVisitor<'a, DB: MerkleMetadataStore> {
    db: &'a DB,
}

impl<'a, 'de, DB: MerkleMetadataStore> Visitor<'de> for LazyNodeVisitor<'a, DB> {
    type Value = LazyNode<'a, DB>;

    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "LazyNode with object reference")
    }

    fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Self::Value, A::Error> {
        let me: Hash = seq
            .next_element()?
            .ok_or_else(|| de::Error::invalid_length(0, &self))?;
        Ok(LazyNode { db: self.db, me })
    }

    fn visit_map<M: MapAccess<'de>>(self, mut map: M) -> Result<Self::Value, M::Error> {
        let mut me: Option<Hash> = None;

        while let Some(key) = map.next_key::<String>()? {
            match key.as_str() {
                "me" => me = Some(map.next_value()?),
                _ => {
                    let _ = map.next_value::<de::IgnoredAny>()?;
                }
            }
        }

        let me = me.ok_or_else(|| de::Error::missing_field("me"))?;
        Ok(LazyNode { db: self.db, me })
    }
}

// -- Option<LazyNode> seed --

pub(crate) struct OptionLazyNodeSeed<'a, DB: MerkleMetadataStore> {
    db: &'a DB,
}

impl<'a, 'de, DB: MerkleMetadataStore> DeserializeSeed<'de> for OptionLazyNodeSeed<'a, DB> {
    type Value = Option<LazyNode<'a, DB>>;

    fn deserialize<D: Deserializer<'de>>(self, deserializer: D) -> Result<Self::Value, D::Error> {
        deserializer.deserialize_option(OptionLazyNodeVisitor { db: self.db })
    }
}

struct OptionLazyNodeVisitor<'a, DB: MerkleMetadataStore> {
    db: &'a DB,
}

impl<'a, 'de, DB: MerkleMetadataStore> Visitor<'de> for OptionLazyNodeVisitor<'a, DB> {
    type Value = Option<LazyNode<'a, DB>>;

    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "an optional LazyNode")
    }

    fn visit_none<E: de::Error>(self) -> Result<Self::Value, E> {
        Ok(None)
    }

    fn visit_unit<E: de::Error>(self) -> Result<Self::Value, E> {
        Ok(None)
    }

    fn visit_some<D: Deserializer<'de>>(self, deserializer: D) -> Result<Self::Value, D::Error> {
        LazyNodeSeed { db: self.db }
            .deserialize(deserializer)
            .map(Some)
    }
}

// -- Vec<LazyNode> seed --

pub(crate) struct VecLazyNodeSeed<'a, DB: MerkleMetadataStore> {
    db: &'a DB,
}

impl<'a, 'de, DB: MerkleMetadataStore> DeserializeSeed<'de> for VecLazyNodeSeed<'a, DB> {
    type Value = Vec<LazyNode<'a, DB>>;

    fn deserialize<D: Deserializer<'de>>(self, deserializer: D) -> Result<Self::Value, D::Error> {
        deserializer.deserialize_seq(VecLazyNodeVisitor { db: self.db })
    }
}

struct VecLazyNodeVisitor<'a, DB: MerkleMetadataStore> {
    db: &'a DB,
}

impl<'a, 'de, DB: MerkleMetadataStore> Visitor<'de> for VecLazyNodeVisitor<'a, DB> {
    type Value = Vec<LazyNode<'a, DB>>;

    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "a sequence of LazyNode")
    }

    fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Self::Value, A::Error> {
        let mut nodes = Vec::with_capacity(seq.size_hint().unwrap_or(0));
        while let Some(node) = seq.next_element_seed(LazyNodeSeed { db: self.db })? {
            nodes.push(node);
        }
        Ok(nodes)
    }
}

// -- MerkleTreeL seed --

pub(crate) struct MerkleTreeLSeed<'a, DB: MerkleMetadataStore> {
    pub(crate) db: &'a DB,
}

impl<'a, 'de, DB: MerkleMetadataStore> DeserializeSeed<'de> for MerkleTreeLSeed<'a, DB> {
    type Value = MerkleTreeL<'a, DB>;

    fn deserialize<D: Deserializer<'de>>(self, deserializer: D) -> Result<Self::Value, D::Error> {
        const VARIANTS: &[&str] = &["Dir", "File"];
        deserializer.deserialize_enum(
            "MerkleTreeL",
            VARIANTS,
            MerkleTreeLEnumVisitor { db: self.db },
        )
    }
}

struct MerkleTreeLEnumVisitor<'a, DB: MerkleMetadataStore> {
    db: &'a DB,
}

impl<'a, 'de, DB: MerkleMetadataStore> Visitor<'de> for MerkleTreeLEnumVisitor<'a, DB> {
    type Value = MerkleTreeL<'a, DB>;

    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "enum MerkleTreeL (Dir or File)")
    }

    fn visit_enum<A: EnumAccess<'de>>(self, data: A) -> Result<Self::Value, A::Error> {
        let (variant_name, variant_access) = data.variant::<String>()?;
        match variant_name.as_str() {
            "Dir" => variant_access.struct_variant(
                &["hash", "name", "parent", "children"],
                DirVariantVisitor { db: self.db },
            ),
            "File" => variant_access.struct_variant(
                &["name", "parent", "content"],
                FileVariantVisitor { db: self.db },
            ),
            other => Err(de::Error::unknown_variant(other, &["Dir", "File"])),
        }
    }
}

struct DirVariantVisitor<'a, DB: MerkleMetadataStore> {
    db: &'a DB,
}

impl<'a, 'de, DB: MerkleMetadataStore> Visitor<'de> for DirVariantVisitor<'a, DB> {
    type Value = MerkleTreeL<'a, DB>;

    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Dir variant of MerkleTreeL")
    }

    fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Self::Value, A::Error> {
        let hash: Hash = seq
            .next_element()?
            .ok_or_else(|| de::Error::invalid_length(0, &self))?;
        let name: String = seq
            .next_element()?
            .ok_or_else(|| de::Error::invalid_length(1, &self))?;
        let parent: Option<LazyNode<'a, DB>> = seq
            .next_element_seed(OptionLazyNodeSeed { db: self.db })?
            .ok_or_else(|| de::Error::invalid_length(2, &self))?;
        let children: Vec<LazyNode<'a, DB>> = seq
            .next_element_seed(VecLazyNodeSeed { db: self.db })?
            .ok_or_else(|| de::Error::invalid_length(3, &self))?;

        Ok(MerkleTreeL::Dir {
            hash,
            name,
            parent,
            children,
        })
    }

    fn visit_map<M: MapAccess<'de>>(self, mut map: M) -> Result<Self::Value, M::Error> {
        let mut hash: Option<Hash> = None;
        let mut name: Option<String> = None;
        let mut parent: Option<Option<LazyNode<'a, DB>>> = None;
        let mut children: Option<Vec<LazyNode<'a, DB>>> = None;

        while let Some(key) = map.next_key::<String>()? {
            match key.as_str() {
                "hash" => hash = Some(map.next_value()?),
                "name" => name = Some(map.next_value()?),
                "parent" => {
                    parent = Some(map.next_value_seed(OptionLazyNodeSeed { db: self.db })?);
                }
                "children" => {
                    children = Some(map.next_value_seed(VecLazyNodeSeed { db: self.db })?);
                }
                _ => {
                    let _ = map.next_value::<de::IgnoredAny>()?;
                }
            }
        }

        Ok(MerkleTreeL::Dir {
            hash: hash.ok_or_else(|| de::Error::missing_field("hash"))?,
            name: name.ok_or_else(|| de::Error::missing_field("name"))?,
            parent: parent.ok_or_else(|| de::Error::missing_field("parent"))?,
            children: children.ok_or_else(|| de::Error::missing_field("children"))?,
        })
    }
}

struct FileVariantVisitor<'a, DB: MerkleMetadataStore> {
    db: &'a DB,
}

impl<'a, 'de, DB: MerkleMetadataStore> Visitor<'de> for FileVariantVisitor<'a, DB> {
    type Value = MerkleTreeL<'a, DB>;

    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "File variant of MerkleTreeL")
    }

    fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Self::Value, A::Error> {
        let name: String = seq
            .next_element()?
            .ok_or_else(|| de::Error::invalid_length(0, &self))?;
        let parent: Option<LazyNode<'a, DB>> = seq
            .next_element_seed(OptionLazyNodeSeed { db: self.db })?
            .ok_or_else(|| de::Error::invalid_length(1, &self))?;
        let content: LazyData<'a, DB> = seq
            .next_element_seed(HasDbSeed { db: self.db })?
            .ok_or_else(|| de::Error::invalid_length(2, &self))?;

        Ok(MerkleTreeL::File {
            name,
            parent,
            content,
        })
    }

    fn visit_map<M: MapAccess<'de>>(self, mut map: M) -> Result<Self::Value, M::Error> {
        let mut name: Option<String> = None;
        let mut parent: Option<Option<LazyNode<'a, DB>>> = None;
        let mut content: Option<LazyData<'a, DB>> = None;

        while let Some(key) = map.next_key::<String>()? {
            match key.as_str() {
                "name" => name = Some(map.next_value()?),
                "parent" => {
                    parent = Some(map.next_value_seed(OptionLazyNodeSeed { db: self.db })?);
                }
                "content" => {
                    content = Some(map.next_value_seed(HasDbSeed { db: self.db })?);
                }
                _ => {
                    let _ = map.next_value::<de::IgnoredAny>()?;
                }
            }
        }

        Ok(MerkleTreeL::File {
            name: name.ok_or_else(|| de::Error::missing_field("name"))?,
            parent: parent.ok_or_else(|| de::Error::missing_field("parent"))?,
            content: content.ok_or_else(|| de::Error::missing_field("content"))?,
        })
    }
}

#[derive(Debug, thiserror::Error)]
pub enum LoadError<DB: MerkleMetadataStore> {
    #[error("Error from Merkle tree data store: {0}")]
    DBError(DB::Error),

    #[error("Cannot determine relative path for hash: {0}")]
    PathError(HexHash),

    #[error("{0}")]
    ReadError(std::io::Error),
}

impl<'a, DB: MerkleMetadataStore> LazyData<'a, DB> {
    /// Reconstructs the relative path to this file node's data
    /// and reads it from storage.
    pub async fn load(&self) -> Result<Vec<u8>, LoadError<DB>> {
        let Some(rel_path) = self.db.path(self.me).map_err(LoadError::DBError)? else {
            return Err(LoadError::PathError(HexHash::from(self.me)));
        };

        let path = AbsolutePath::from(self.db.repository(), &rel_path);
        tokio::fs::read(path.as_path())
            .await
            .map_err(LoadError::ReadError)
    }

    pub fn hash(&self) -> Hash {
        self.me
    }
}

impl<'a, DB: MerkleMetadataStore> MerkleTreeL<'a, DB> {
    pub fn hash(&self) -> Hash {
        match self {
            MerkleTreeL::Dir { hash, .. } => *hash,
            MerkleTreeL::File { content, .. } => content.hash(),
        }
    }

    pub fn name(&self) -> &str {
        match self {
            MerkleTreeL::Dir { name, .. } => name,
            MerkleTreeL::File { name, .. } => name,
        }
    }

    pub fn parent(&self) -> Option<&LazyNode<DB>> {
        match self {
            MerkleTreeL::Dir { parent, .. } => parent,
            MerkleTreeL::File { parent, .. } => parent,
        }
        .as_ref()
    }
}

pub struct Root<'a, DB: MerkleMetadataStore> {
    root: AbsolutePath,
    hash: Hash,
    children: Vec<LazyNode<'a, DB>>,
}

impl<'a, DB: MerkleMetadataStore> Root<'a, DB> {
    pub fn hash(&self) -> Hash {
        self.hash
    }

    pub fn children(&self) -> impl Iterator<Item = &LazyNode<'a, DB>> {
        self.children.iter()
    }
}
