use crate::error::OxenError;

/// Stored in the `nodes` LMDB database.
/// Key: MerkleHash.to_le_bytes() (16 bytes)
/// Value: [dtype: u8][parent_id: u128 LE][data_len: u32 LE][msgpack_data: data_len bytes]
pub struct NodeRecord {
    pub dtype: u8,
    pub parent_id: u128,
    pub data: Vec<u8>,
}

impl NodeRecord {
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(1 + 16 + 4 + self.data.len());
        buf.push(self.dtype);
        buf.extend_from_slice(&self.parent_id.to_le_bytes());
        buf.extend_from_slice(&(self.data.len() as u32).to_le_bytes());
        buf.extend_from_slice(&self.data);
        buf
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, OxenError> {
        if bytes.len() < 21 {
            return Err(OxenError::basic_str(
                "NodeRecord::from_bytes: insufficient data",
            ));
        }
        let dtype = bytes[0];
        let parent_id = u128::from_le_bytes(bytes[1..17].try_into().unwrap());
        let data_len = u32::from_le_bytes(bytes[17..21].try_into().unwrap()) as usize;
        if bytes.len() < 21 + data_len {
            return Err(OxenError::basic_str(
                "NodeRecord::from_bytes: data truncated",
            ));
        }
        let data = bytes[21..21 + data_len].to_vec();
        Ok(Self {
            dtype,
            parent_id,
            data,
        })
    }
}

/// One child entry within the children database value.
pub struct ChildEntry {
    pub dtype: u8,
    pub hash: u128,
    pub data: Vec<u8>,
}

impl ChildEntry {
    /// Serialize a vec of children into a single byte buffer.
    /// Format: [num_children: u32 LE][for each: dtype: u8, hash: u128 LE, data_len: u32 LE, data]
    pub fn vec_to_bytes(children: &[ChildEntry]) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.extend_from_slice(&(children.len() as u32).to_le_bytes());
        for child in children {
            buf.push(child.dtype);
            buf.extend_from_slice(&child.hash.to_le_bytes());
            buf.extend_from_slice(&(child.data.len() as u32).to_le_bytes());
            buf.extend_from_slice(&child.data);
        }
        buf
    }

    /// Deserialize from the byte buffer back into a vec.
    pub fn vec_from_bytes(bytes: &[u8]) -> Result<Vec<Self>, OxenError> {
        if bytes.len() < 4 {
            return Err(OxenError::basic_str(
                "ChildEntry::vec_from_bytes: insufficient data for count",
            ));
        }
        let num_children = u32::from_le_bytes(bytes[0..4].try_into().unwrap()) as usize;
        let mut entries = Vec::with_capacity(num_children);
        let mut offset = 4;
        for _ in 0..num_children {
            if offset + 21 > bytes.len() {
                return Err(OxenError::basic_str(
                    "ChildEntry::vec_from_bytes: data truncated",
                ));
            }
            let dtype = bytes[offset];
            offset += 1;
            let hash = u128::from_le_bytes(bytes[offset..offset + 16].try_into().unwrap());
            offset += 16;
            let data_len =
                u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap()) as usize;
            offset += 4;
            if offset + data_len > bytes.len() {
                return Err(OxenError::basic_str(
                    "ChildEntry::vec_from_bytes: child data truncated",
                ));
            }
            let data = bytes[offset..offset + data_len].to_vec();
            offset += data_len;
            entries.push(ChildEntry { dtype, hash, data });
        }
        Ok(entries)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_node_record_roundtrip() {
        let record = NodeRecord {
            dtype: 1,
            parent_id: 12345678,
            data: vec![10, 20, 30, 40],
        };
        let bytes = record.to_bytes();
        let decoded = NodeRecord::from_bytes(&bytes).unwrap();
        assert_eq!(decoded.dtype, 1);
        assert_eq!(decoded.parent_id, 12345678);
        assert_eq!(decoded.data, vec![10, 20, 30, 40]);
    }

    #[test]
    fn test_child_entry_roundtrip() {
        let children = vec![
            ChildEntry {
                dtype: 3,
                hash: 111,
                data: vec![1, 2, 3],
            },
            ChildEntry {
                dtype: 1,
                hash: 222,
                data: vec![4, 5],
            },
        ];
        let bytes = ChildEntry::vec_to_bytes(&children);
        let decoded = ChildEntry::vec_from_bytes(&bytes).unwrap();
        assert_eq!(decoded.len(), 2);
        assert_eq!(decoded[0].dtype, 3);
        assert_eq!(decoded[0].hash, 111);
        assert_eq!(decoded[0].data, vec![1, 2, 3]);
        assert_eq!(decoded[1].dtype, 1);
        assert_eq!(decoded[1].hash, 222);
        assert_eq!(decoded[1].data, vec![4, 5]);
    }

    #[test]
    fn test_empty_children_roundtrip() {
        let children: Vec<ChildEntry> = vec![];
        let bytes = ChildEntry::vec_to_bytes(&children);
        let decoded = ChildEntry::vec_from_bytes(&bytes).unwrap();
        assert!(decoded.is_empty());
    }
}
