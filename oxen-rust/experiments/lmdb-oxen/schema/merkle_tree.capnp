@0xf8a3b2c1d4e5f6a7;

# Cap'n Proto schema for MerkleTreeNode and related structures
# This schema is designed for benchmarking deserialization performance

# MerkleHash is a u128 value
struct MerkleHash {
  value @0 :UInt128;
}

# Helper struct for u128 since Cap'n Proto doesn't have native u128
struct UInt128 {
  high @0 :UInt64;
  low @1 :UInt64;
}

# MerkleTreeNodeType enum
enum MerkleTreeNodeType {
  commit @0;
  dir @1;
  vnode @2;
  file @3;
  fileChunk @4;
}

# EntryDataType enum
enum EntryDataType {
  dir @0;
  text @1;
  image @2;
  video @3;
  audio @4;
  tabular @5;
  binary @6;
}

# FileChunkType enum
enum FileChunkType {
  singleFile @0;
  chunked @1;
}

# FileStorageType enum
enum FileStorageType {
  disk @0;
  s3 @1;
}

# GenericMetadata - simplified as a union of metadata types
struct GenericMetadata {
  union {
    metadataDir @0 :MetadataDir;
    metadataText @1 :MetadataText;
    metadataImage @2 :MetadataImage;
    metadataVideo @3 :MetadataVideo;
    metadataAudio @4 :MetadataAudio;
    metadataTabular @5 :MetadataTabular;
  }
}

# Placeholder metadata structures (simplified)
struct MetadataDir {
  # Add fields as needed
  placeholder @0 :Text;
}

struct MetadataText {
  placeholder @0 :Text;
}

struct MetadataImage {
  width @0 :UInt32;
  height @1 :UInt32;
}

struct MetadataVideo {
  width @0 :UInt32;
  height @1 :UInt32;
  duration @2 :Float64;
}

struct MetadataAudio {
  duration @0 :Float64;
}

struct MetadataTabular {
  numRows @0 :UInt64;
  numCols @1 :UInt64;
}

# FileNodeData structure
struct FileNodeData {
  nodeType @0 :MerkleTreeNodeType;
  name @1 :Text;
  metadataHash @2 :MerkleHash;
  hasMetadataHash @3 :Bool;
  hash @4 :MerkleHash;
  combinedHash @5 :MerkleHash;
  numBytes @6 :UInt64;
  lastCommitId @7 :MerkleHash;
  lastModifiedSeconds @8 :Int64;
  lastModifiedNanoseconds @9 :UInt32;
  dataType @10 :EntryDataType;
  metadata @11 :GenericMetadata;
  hasMetadata @12 :Bool;
  mimeType @13 :Text;
  extension @14 :Text;
  chunkHashes @15 :List(UInt128);
  chunkType @16 :FileChunkType;
  storageBackend @17 :FileStorageType;
}

# DirNodeData structure
struct DirNodeData {
  nodeType @0 :MerkleTreeNodeType;
  name @1 :Text;
  hash @2 :MerkleHash;
  numEntries @3 :UInt64;
  numBytes @4 :UInt64;
  lastCommitId @5 :MerkleHash;
  lastModifiedSeconds @6 :Int64;
  lastModifiedNanoseconds @7 :UInt32;
  dataTypeCounts @8 :List(DataTypeCount);
  dataTypeSizes @9 :List(DataTypeSize);
}

struct DataTypeCount {
  dataType @0 :Text;
  count @1 :UInt64;
}

struct DataTypeSize {
  dataType @0 :Text;
  size @1 :UInt64;
}

# VNodeData structure
struct VNodeData {
  hash @0 :MerkleHash;
  nodeType @1 :MerkleTreeNodeType;
  numEntries @2 :UInt64;
}

# FileChunkNode structure
struct FileChunkNode {
  data @0 :Data;
  nodeType @1 :MerkleTreeNodeType;
  hash @2 :MerkleHash;
}

# CommitNodeData structure
struct CommitNodeData {
  hash @0 :MerkleHash;
  nodeType @1 :MerkleTreeNodeType;
  parentIds @2 :List(MerkleHash);
  message @3 :Text;
  author @4 :Text;
  email @5 :Text;
  timestampSeconds @6 :Int64;
  timestampNanoseconds @7 :UInt32;
}

# EMerkleTreeNode - union of all node types
struct EMerkleTreeNode {
  union {
    file @0 :FileNodeData;
    directory @1 :DirNodeData;
    vnode @2 :VNodeData;
    fileChunk @3 :FileChunkNode;
    commit @4 :CommitNodeData;
  }
}

# Main MerkleTreeNode structure
struct MerkleTreeNode {
  hash @0 :MerkleHash;
  node @1 :EMerkleTreeNode;
  parentId @2 :MerkleHash;
  hasParentId @3 :Bool;
  children @4 :List(MerkleTreeNode);
}
