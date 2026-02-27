use std::io::{self, BufReader, BufWriter, Write};
use std::path::Path;

use crate::error::OxenError;
use crate::lfs::config::LfsConfig;
use crate::lfs::filter;

/// pkt-line helpers for the Git long-running filter protocol.
pub mod pkt_line {
    use std::io::{self, BufRead, Write};

    const MAX_PKT_PAYLOAD: usize = 65516;

    /// Write a pkt-line text packet (adds newline automatically).
    pub fn write_text(w: &mut impl Write, text: &str) -> io::Result<()> {
        let payload = format!("{text}\n");
        let len = payload.len() + 4; // 4-byte length prefix
        write!(w, "{len:04x}{payload}")?;
        Ok(())
    }

    /// Write binary data as one or more pkt-line packets.
    pub fn write_binary(w: &mut impl Write, data: &[u8]) -> io::Result<()> {
        for chunk in data.chunks(MAX_PKT_PAYLOAD) {
            let len = chunk.len() + 4;
            write!(w, "{len:04x}")?;
            w.write_all(chunk)?;
        }
        Ok(())
    }

    /// Write a flush packet (0000).
    pub fn write_flush(w: &mut impl Write) -> io::Result<()> {
        w.write_all(b"0000")?;
        Ok(())
    }

    /// Read one pkt-line packet. Returns `None` on flush (0000) or EOF.
    pub fn read_packet(r: &mut impl BufRead) -> io::Result<Option<Vec<u8>>> {
        let mut len_buf = [0u8; 4];
        match r.read_exact(&mut len_buf) {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
            Err(e) => return Err(e),
        }

        let len_str = std::str::from_utf8(&len_buf)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "invalid pkt-line length"))?;

        let len = usize::from_str_radix(len_str, 16)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "invalid pkt-line hex"))?;

        if len == 0 {
            // Flush packet.
            return Ok(None);
        }

        if len < 4 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "pkt-line length too small",
            ));
        }

        let payload_len = len - 4;
        let mut payload = vec![0u8; payload_len];
        r.read_exact(&mut payload)?;
        Ok(Some(payload))
    }

    /// Read all packets until flush, concatenating their payloads.
    pub fn read_until_flush(r: &mut impl BufRead) -> io::Result<Vec<u8>> {
        let mut result = Vec::new();
        while let Some(pkt) = read_packet(r)? {
            result.extend_from_slice(&pkt);
        }
        Ok(result)
    }

    /// Read all text lines until flush. Returns each line trimmed of trailing newline.
    pub fn read_lines_until_flush(r: &mut impl BufRead) -> io::Result<Vec<String>> {
        let mut lines = Vec::new();
        while let Some(pkt) = read_packet(r)? {
            let text = String::from_utf8_lossy(&pkt);
            lines.push(text.trim_end_matches('\n').to_string());
        }
        Ok(lines)
    }

    /// Read text key=value pairs until flush (lines without `=` are skipped).
    pub fn read_text_pairs_until_flush(r: &mut impl BufRead) -> io::Result<Vec<(String, String)>> {
        let lines = read_lines_until_flush(r)?;
        let pairs = lines
            .into_iter()
            .filter_map(|line| {
                line.split_once('=')
                    .map(|(k, v)| (k.to_string(), v.to_string()))
            })
            .collect();
        Ok(pairs)
    }
}

/// Run the long-running Git filter process on stdin/stdout.
///
/// This implements the protocol described in `gitattributes(5)` under
/// "Long Running Filter Process".
pub fn run_filter_process(versions_dir: &Path) -> Result<(), OxenError> {
    let stdin = io::stdin();
    let stdout = io::stdout();
    let mut reader = BufReader::new(stdin.lock());
    let mut writer = BufWriter::new(stdout.lock());

    // Ensure versions dir exists (may be missing on a fresh clone).
    std::fs::create_dir_all(versions_dir).ok();

    // Derive repo_root: versions_dir is .oxen/versions, so two parents up.
    let oxen_dir = versions_dir.parent().unwrap_or(Path::new("."));
    let repo_root = oxen_dir.parent().unwrap_or(Path::new("."));

    let lfs_config = LfsConfig::load(oxen_dir)?;

    // --- Handshake ---
    // Phase 1: Git sends welcome + version(s) in one flush group.
    //   packet: git-filter-client\n
    //   packet: version=2\n
    //   packet: 0000
    let welcome_lines = pkt_line::read_lines_until_flush(&mut reader)?;

    if !welcome_lines.iter().any(|l| l == "git-filter-client") {
        return Err(OxenError::basic_str(
            "expected git-filter-client in handshake",
        ));
    }

    // Respond with welcome + chosen version in one flush group.
    pkt_line::write_text(&mut writer, "git-filter-server")?;
    pkt_line::write_text(&mut writer, "version=2")?;
    pkt_line::write_flush(&mut writer)?;
    writer.flush()?;

    // Phase 2: Git sends its capabilities (e.g. capability=clean,
    // capability=smudge) in one flush group. We read and discard them
    // because the protocol requires consuming this flush group before
    // we can advertise our own capabilities. We unconditionally
    // advertise both clean and smudge regardless of what Git offers.
    let _caps = pkt_line::read_text_pairs_until_flush(&mut reader)?;

    // Respond with the capabilities we support.
    pkt_line::write_text(&mut writer, "capability=clean")?;
    pkt_line::write_text(&mut writer, "capability=smudge")?;
    pkt_line::write_flush(&mut writer)?;
    writer.flush()?;

    // Get a handle to the current tokio runtime. The CLI's main() already
    // starts one, so we must not create a second. We use block_in_place +
    // block_on to run async version-store ops from this synchronous context.
    let handle = tokio::runtime::Handle::current();

    // --- Per-file loop ---
    loop {
        // Read command + pathname (key=value pairs until flush).
        let pairs = match pkt_line::read_text_pairs_until_flush(&mut reader) {
            Ok(p) if p.is_empty() => break, // EOF / no more commands
            Ok(p) => p,
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => break,
            Err(e) => return Err(OxenError::IO(e)),
        };

        let command = pairs
            .iter()
            .find(|(k, _)| k == "command")
            .map(|(_, v)| v.as_str())
            .unwrap_or("");

        // Read content until flush.
        let content = pkt_line::read_until_flush(&mut reader)?;

        let result = match command {
            "clean" => tokio::task::block_in_place(|| {
                handle.block_on(filter::clean(versions_dir, &content))
            })?,
            "smudge" => tokio::task::block_in_place(|| {
                handle.block_on(filter::smudge(
                    versions_dir,
                    repo_root,
                    &lfs_config,
                    &content,
                ))
            })?,
            other => {
                log::warn!("oxen lfs filter-process: unknown command '{other}', passing through");
                content
            }
        };

        // Write status=success, flush, content, flush, flush.
        pkt_line::write_text(&mut writer, "status=success")?;
        pkt_line::write_flush(&mut writer)?;
        pkt_line::write_binary(&mut writer, &result)?;
        pkt_line::write_flush(&mut writer)?;
        pkt_line::write_flush(&mut writer)?;
        writer.flush()?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::pkt_line::*;
    use std::io::Cursor;

    #[test]
    fn test_pkt_line_roundtrip_text() {
        let mut buf = Vec::new();
        write_text(&mut buf, "hello").unwrap();
        write_flush(&mut buf).unwrap();

        let mut reader = Cursor::new(buf);
        let pkt = read_packet(&mut reader).unwrap().unwrap();
        assert_eq!(String::from_utf8_lossy(&pkt), "hello\n");

        // Next read should be flush => None
        let flush = read_packet(&mut reader).unwrap();
        assert!(flush.is_none());
    }

    #[test]
    fn test_pkt_line_binary() {
        let data = vec![0u8; 100];
        let mut buf = Vec::new();
        write_binary(&mut buf, &data).unwrap();
        write_flush(&mut buf).unwrap();

        let mut reader = Cursor::new(buf);
        let result = read_until_flush(&mut reader).unwrap();
        assert_eq!(result, data);
    }

    #[test]
    fn test_read_text_pairs() {
        let mut buf = Vec::new();
        write_text(&mut buf, "command=clean").unwrap();
        write_text(&mut buf, "pathname=test.bin").unwrap();
        write_flush(&mut buf).unwrap();

        let mut reader = Cursor::new(buf);
        let pairs = read_text_pairs_until_flush(&mut reader).unwrap();
        assert_eq!(pairs.len(), 2);
        assert_eq!(pairs[0], ("command".to_string(), "clean".to_string()));
        assert_eq!(pairs[1], ("pathname".to_string(), "test.bin".to_string()));
    }

    #[test]
    fn test_read_lines_includes_non_pairs() {
        // Git sends "git-filter-client" (no =) plus "version=2" in one group.
        let mut buf = Vec::new();
        write_text(&mut buf, "git-filter-client").unwrap();
        write_text(&mut buf, "version=2").unwrap();
        write_flush(&mut buf).unwrap();

        let mut reader = Cursor::new(buf);
        let lines = read_lines_until_flush(&mut reader).unwrap();
        assert_eq!(lines.len(), 2);
        assert_eq!(lines[0], "git-filter-client");
        assert_eq!(lines[1], "version=2");
    }
}
