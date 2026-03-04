use std::fmt;

/// First line of every Oxen LFS pointer file.
pub const POINTER_VERSION_LINE: &str = "version https://oxen.ai/spec/v1";

/// Hash algorithm identifier used in pointer files.
pub const HASH_ALGO: &str = "xxh3";

/// Pointer files should never exceed this size in bytes.
pub const MAX_POINTER_SIZE: usize = 200;

/// Represents an Oxen LFS pointer — a small stand-in stored in Git
/// that references content kept in the Oxen version store.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PointerFile {
    /// 32-char lowercase hex hash (no algorithm prefix).
    pub oid: String,
    /// Size in bytes of the original content.
    pub size: u64,
}

impl PointerFile {
    pub fn new(hash: &str, size: u64) -> Self {
        Self {
            oid: hash.to_string(),
            size,
        }
    }

    /// Serialize to the canonical pointer text (UTF-8, newline-terminated).
    pub fn encode(&self) -> Vec<u8> {
        self.to_string().into_bytes()
    }

    /// Try to parse a byte slice as a pointer file.
    /// Returns `None` when the data is not a valid pointer.
    pub fn decode(data: &[u8]) -> Option<Self> {
        if data.len() > MAX_POINTER_SIZE {
            return None;
        }

        let text = std::str::from_utf8(data).ok()?;
        let mut lines = text.lines();

        // Line 1: version
        let version_line = lines.next()?;
        if version_line != POINTER_VERSION_LINE {
            return None;
        }

        // Line 2: oid <algo>:<hex>
        let oid_line = lines.next()?;
        let oid_value = oid_line.strip_prefix("oid ")?;
        let hash = oid_value.strip_prefix(&format!("{HASH_ALGO}:"))?;

        // Validate hex string (should be 32 chars for xxh3_128)
        if hash.len() != 32 || !hash.chars().all(|c| c.is_ascii_hexdigit()) {
            return None;
        }

        // Line 3: size <decimal>
        let size_line = lines.next()?;
        let size_value = size_line.strip_prefix("size ")?;
        let size: u64 = size_value.parse().ok()?;

        // No extra lines allowed (other than a trailing newline which .lines() skips)
        if lines.next().is_some() {
            return None;
        }

        Some(Self {
            oid: hash.to_string(),
            size,
        })
    }

    /// Quick check: is this byte slice a valid Oxen LFS pointer?
    pub fn is_pointer(data: &[u8]) -> bool {
        if data.len() > MAX_POINTER_SIZE {
            return false;
        }
        // Fast path: check the version prefix before full parse.
        data.starts_with(POINTER_VERSION_LINE.as_bytes()) && Self::decode(data).is_some()
    }
}

impl fmt::Display for PointerFile {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}\noid {}:{}\nsize {}\n",
            POINTER_VERSION_LINE, HASH_ALGO, self.oid, self.size,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pointer_roundtrip() {
        let ptr = PointerFile::new("a1b2c3d4e5f6a7b8a1b2c3d4e5f6a7b8", 1234567890);
        let encoded = ptr.encode();
        let decoded = PointerFile::decode(&encoded).expect("should decode");
        assert_eq!(ptr, decoded);
    }

    #[test]
    fn test_pointer_format() {
        let ptr = PointerFile::new("a1b2c3d4e5f6a7b8a1b2c3d4e5f6a7b8", 42);
        let text = String::from_utf8(ptr.encode()).unwrap();
        assert_eq!(
            text,
            "version https://oxen.ai/spec/v1\noid xxh3:a1b2c3d4e5f6a7b8a1b2c3d4e5f6a7b8\nsize 42\n"
        );
    }

    #[test]
    fn test_is_pointer_true() {
        let ptr = PointerFile::new("a1b2c3d4e5f6a7b8a1b2c3d4e5f6a7b8", 100);
        assert!(PointerFile::is_pointer(&ptr.encode()));
    }

    #[test]
    fn test_is_pointer_false_random_data() {
        assert!(!PointerFile::is_pointer(
            b"hello world, this is not a pointer"
        ));
    }

    #[test]
    fn test_is_pointer_false_too_large() {
        let big = vec![b'x'; MAX_POINTER_SIZE + 1];
        assert!(!PointerFile::is_pointer(&big));
    }

    #[test]
    fn test_decode_rejects_bad_hash_length() {
        let bad = b"version https://oxen.ai/spec/v1\noid xxh3:abc123\nsize 10\n";
        assert!(PointerFile::decode(bad).is_none());
    }

    #[test]
    fn test_decode_rejects_extra_lines() {
        let bad = b"version https://oxen.ai/spec/v1\noid xxh3:a1b2c3d4e5f6a7b8a1b2c3d4e5f6a7b8\nsize 10\nextra\n";
        assert!(PointerFile::decode(bad).is_none());
    }

    #[test]
    fn test_decode_rejects_wrong_version() {
        let bad = b"version https://git-lfs.github.com/spec/v1\noid xxh3:a1b2c3d4e5f6a7b8a1b2c3d4e5f6a7b8\nsize 10\n";
        assert!(PointerFile::decode(bad).is_none());
    }

    #[test]
    fn test_decode_rejects_wrong_algorithm() {
        let bad = b"version https://oxen.ai/spec/v1\noid sha256:a1b2c3d4e5f6a7b8a1b2c3d4e5f6a7b8\nsize 10\n";
        assert!(PointerFile::decode(bad).is_none());
    }
}
