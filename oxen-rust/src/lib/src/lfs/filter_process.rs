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

    /// Read text key=value pairs until flush.
    pub fn read_text_pairs_until_flush(r: &mut impl BufRead) -> io::Result<Vec<(String, String)>> {
        let mut pairs = Vec::new();
        while let Some(pkt) = read_packet(r)? {
            let text = String::from_utf8_lossy(&pkt);
            let text = text.trim_end_matches('\n');
            if let Some((key, value)) = text.split_once('=') {
                pairs.push((key.to_string(), value.to_string()));
            }
        }
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

    let lfs_config = LfsConfig::load(versions_dir.parent().unwrap_or(Path::new(".")))?;

    // --- Handshake ---
    // Read welcome message.
    let welcome = pkt_line::read_until_flush(&mut reader)?;
    let welcome_str = String::from_utf8_lossy(&welcome);
    if !welcome_str.contains("git-filter-client") {
        return Err(OxenError::basic_str("expected git-filter-client handshake"));
    }

    // Read version.
    let _version = pkt_line::read_until_flush(&mut reader)?;

    // Send our welcome + version.
    pkt_line::write_text(&mut writer, "git-filter-server")?;
    pkt_line::write_flush(&mut writer)?;
    pkt_line::write_text(&mut writer, "version=2")?;
    pkt_line::write_flush(&mut writer)?;
    writer.flush()?;

    // Read capabilities.
    let _caps = pkt_line::read_text_pairs_until_flush(&mut reader)?;

    // Advertise our capabilities.
    pkt_line::write_text(&mut writer, "capability=clean")?;
    pkt_line::write_text(&mut writer, "capability=smudge")?;
    pkt_line::write_flush(&mut writer)?;
    writer.flush()?;

    // Build a tokio runtime for async version-store operations.
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|e| OxenError::basic_str(format!("failed to build tokio runtime: {e}")))?;

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
            "clean" => rt.block_on(filter::clean(versions_dir, &content))?,
            "smudge" => rt.block_on(filter::smudge(versions_dir, &lfs_config, &content))?,
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
}
