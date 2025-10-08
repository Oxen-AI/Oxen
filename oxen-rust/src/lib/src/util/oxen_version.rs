use crate::error::OxenError;
use std::fmt;
use std::str::FromStr;
pub struct OxenVersion {
    pub major: u32,
    pub minor: u32,
    pub patch: u32,
}

impl FromStr for OxenVersion {
    type Err = OxenError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let parts: Vec<&str> = s.split(['.', '-']).collect();
        if parts.len() < 3 || parts.len() > 4 {
            return Err(OxenError::basic_str(format!(
                "Invalid version string: {s}"
            )));
        }
        let major = parts[0].parse::<u32>()?;
        let minor = parts[1].parse::<u32>()?;
        let patch = parts[2].parse::<u32>()?;
        Ok(OxenVersion {
            major,
            minor,
            patch,
        })
    }
}

impl fmt::Debug for OxenVersion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}.{}.{}", self.major, self.minor, self.patch)
    }
}

impl fmt::Display for OxenVersion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}.{}.{}", self.major, self.minor, self.patch)
    }
}

// Ignore everything after patch (beta, etc.)
impl Eq for OxenVersion {}
impl PartialEq for OxenVersion {
    fn eq(&self, other: &Self) -> bool {
        self.major == other.major && self.minor == other.minor && self.patch == other.patch
    }
}

impl PartialOrd for OxenVersion {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for OxenVersion {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        match self.major.cmp(&other.major) {
            std::cmp::Ordering::Equal => match self.minor.cmp(&other.minor) {
                std::cmp::Ordering::Equal => self.patch.cmp(&other.patch),
                o => o,
            },
            o => o,
        }
    }
}
