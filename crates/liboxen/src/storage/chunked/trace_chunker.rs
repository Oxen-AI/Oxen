//! Structure-anchored chunking for line-delimited JSON traces
//! ([`ChunkerId::TRACE_JSONL_V1`]).
//!
//! Built for tables whose rows are long-lived records that grow by appending to a
//! nested array — chat sessions and agent traces (`{"uuid", "messages": [...],
//! "source"}` and the like) committed repeatedly as the arrays gain entries.
//! Byte-level CDC amplifies that workload badly: a chunk spanning many rows dies
//! when any one of them changes. This chunker cuts only at structural anchors so
//! that dedup granularity follows the data's own edit granularity:
//!
//! - **Row anchors.** A cut is taken at a row boundary (after `\n`) when the row
//!   that just ended reached [`ROW_ISOLATE_MIN`] bytes, or when that much has
//!   accumulated across smaller rows. Every row at least that large therefore both
//!   starts and ends at a chunk boundary, and its chunks are a function of the
//!   row's bytes alone — an unchanged row produces identical chunks *wherever it
//!   sits in the file*, making dedup invariant to row insertion, deletion,
//!   reordering, and sorting. Smaller rows coalesce with their neighbors.
//! - **Element anchors.** Inside a row, a cut is taken after an element separator
//!   of a depth-2 array (the elements of `messages: [...]` in a JSONL row) once at
//!   least [`INTRA_ROW_TARGET`] bytes have accumulated. Cuts depend only on the
//!   bytes already seen, so a row that grows by appending keeps every chunk before
//!   its tail byte-identical — an appended session costs its tail chunk plus the
//!   new messages, not the whole row.
//!
//! Any byte stream is safe input. The structural scanner resets at each newline, so
//! malformed lines never confuse parsing of later lines; input with no anchors at
//! all (binary, single giant line) degrades to forced cuts at the format's maximum
//! chunk size. Correctness (chunks tile the input) never depends on the input being
//! valid JSONL — only dedup quality does.
//!
//! The cut rule and both thresholds are frozen by [`ChunkerId::TRACE_JSONL_V1`]:
//! changing any of them ships as a new chunker ID, never an in-place change.

use std::collections::VecDeque;
use std::io::Read;

use super::MAX_CHUNK_SIZE;
use super::chunker::{Chunker, RawChunk};
use super::error::ChunkedError;
use super::registry::ChunkerId;

/// Row-isolation floor. A row at least this large always starts and ends at chunk
/// boundaries (position-independent chunks); smaller rows coalesce until this many
/// bytes have accumulated at a row boundary.
const ROW_ISOLATE_MIN: usize = 1024;

/// Minimum accumulated bytes for a cut at a depth-2 array element boundary inside a
/// row. Bounds the tail-chunk cost of appending to a row's array.
const INTRA_ROW_TARGET: usize = 8 * 1024;

/// How many bytes are read from the input per scan step.
const READ_BUF_SIZE: usize = 64 * 1024;

/// Tracks just enough JSON structure to recognize element separators of depth-2
/// arrays: string state and a container stack (object/array) up to a small cap.
///
/// Resets at every newline, so each line is scanned independently and a malformed
/// line cannot corrupt anchor detection for the lines after it.
struct LineScanner {
    in_string: bool,
    escaped: bool,
    /// Container kind per open nesting level, innermost last; `true` = array.
    /// Levels beyond [`Self::STACK_CAP`] are only counted (no anchors that deep).
    stack: Vec<bool>,
    /// Open containers beyond the stack cap.
    overflow: u32,
}

impl LineScanner {
    const STACK_CAP: usize = 8;

    fn new() -> Self {
        Self {
            in_string: false,
            escaped: false,
            stack: Vec::with_capacity(Self::STACK_CAP),
            overflow: 0,
        }
    }

    fn reset(&mut self) {
        self.in_string = false;
        self.escaped = false;
        self.stack.clear();
        self.overflow = 0;
    }

    /// Consume one byte; report whether the byte is an element separator (`,`) of a
    /// depth-2 array — the anchor after which an intra-row cut may be taken.
    fn is_element_separator(&mut self, byte: u8) -> bool {
        if self.in_string {
            match byte {
                _ if self.escaped => self.escaped = false,
                b'\\' => self.escaped = true,
                b'"' => self.in_string = false,
                _ => {}
            }
            return false;
        }
        match byte {
            b'"' => self.in_string = true,
            b'{' | b'[' => {
                if self.stack.len() < Self::STACK_CAP {
                    self.stack.push(byte == b'[');
                } else {
                    self.overflow = self.overflow.saturating_add(1);
                }
            }
            b'}' | b']' => {
                if self.overflow > 0 {
                    self.overflow -= 1;
                } else {
                    self.stack.pop();
                }
            }
            b',' => {
                // Depth-2 array element separator: root object -> array field.
                return self.overflow == 0 && self.stack == [false, true];
            }
            _ => {}
        }
        false
    }
}

/// Streaming iterator over the trace-chunked input: bounded memory (one pending
/// chunk plus one read buffer), cuts decided byte-by-byte with no lookahead.
struct TraceChunkIter<'r> {
    reader: Box<dyn Read + Send + 'r>,
    scanner: LineScanner,
    /// Bytes of the chunk currently being accumulated.
    pending: Vec<u8>,
    /// File offset of the first byte in `pending`.
    pending_offset: u64,
    /// Total bytes of the current row seen so far, across cuts. Rows that reach
    /// [`ROW_ISOLATE_MIN`] always cut at their end, so a big row's chunks never
    /// leak into the next row.
    row_len: usize,
    /// Bytes read from the input but not yet scanned.
    buf: VecDeque<u8>,
    eof: bool,
    done: bool,
}

impl TraceChunkIter<'_> {
    fn take_pending(&mut self) -> RawChunk {
        let data = std::mem::take(&mut self.pending);
        let chunk = RawChunk {
            offset: self.pending_offset,
            data,
        };
        self.pending_offset += chunk.data.len() as u64;
        chunk
    }
}

impl Iterator for TraceChunkIter<'_> {
    type Item = Result<RawChunk, ChunkedError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }
        loop {
            // Refill the scan buffer before draining it.
            if self.buf.is_empty() && !self.eof {
                let mut read_buf = vec![0u8; READ_BUF_SIZE];
                match self.reader.read(&mut read_buf) {
                    Ok(0) => self.eof = true,
                    Ok(n) => self.buf.extend(&read_buf[..n]),
                    Err(err) => {
                        self.done = true;
                        return Some(Err(ChunkedError::ChunkRead(err)));
                    }
                }
            }

            let Some(byte) = self.buf.pop_front() else {
                // EOF: flush whatever is pending, exactly once.
                self.done = true;
                if self.pending.is_empty() {
                    return None;
                }
                return Some(Ok(self.take_pending()));
            };

            let element_separator = self.scanner.is_element_separator(byte);
            let row_end = byte == b'\n';
            if row_end {
                self.scanner.reset();
            }
            self.pending.push(byte);
            self.row_len += 1;

            // The cut rule frozen by TRACE_JSONL_V1, in priority order. Each cut
            // depends only on bytes already consumed — never on lookahead — so a
            // row's chunks are stable under anything appended after it. A row that
            // reached the isolation floor always cuts at its end (even if the tail
            // is small), so its chunks never glue onto the next row — the property
            // that makes chunks position-independent under row reordering.
            let cut = self.pending.len() >= MAX_CHUNK_SIZE as usize
                || (row_end
                    && (self.row_len >= ROW_ISOLATE_MIN || self.pending.len() >= ROW_ISOLATE_MIN))
                || (element_separator && self.pending.len() >= INTRA_ROW_TARGET);
            if row_end {
                self.row_len = 0;
            }
            if cut {
                return Some(Ok(self.take_pending()));
            }
        }
    }
}

/// Structure-anchored JSONL trace chunker — the boundary function frozen as
/// [`ChunkerId::TRACE_JSONL_V1`]. See the module docs for the cut rule and the
/// dedup properties it buys.
pub struct TraceJsonlChunker;

impl Chunker for TraceJsonlChunker {
    fn id(&self) -> ChunkerId {
        ChunkerId::TRACE_JSONL_V1
    }

    fn chunk<'r>(
        &self,
        reader: Box<dyn Read + Send + 'r>,
    ) -> Box<dyn Iterator<Item = Result<RawChunk, ChunkedError>> + Send + 'r> {
        Box::new(TraceChunkIter {
            reader,
            scanner: LineScanner::new(),
            pending: Vec::new(),
            pending_offset: 0,
            row_len: 0,
            buf: VecDeque::new(),
            eof: false,
            done: false,
        })
    }
}

/// Bytes sniffed from the head of the input to decide the auto chunker's
/// delegation.
const AUTO_SNIFF_BYTES: usize = 64 * 1024;

/// Average-row-size threshold for the auto chunker: at or above this, rows are
/// long traces whose structural chunking pays for itself; below it, small rows
/// compress and dedup better through generic byte-CDC windows plus the shared
/// dictionary.
const AUTO_ROW_THRESHOLD: usize = 4 * 1024;

/// Content-adaptive chunking for line-delimited JSON
/// ([`ChunkerId::TRACE_AUTO_V1`]): sniff the first [`AUTO_SNIFF_BYTES`] of the
/// stream, then delegate the whole stream to the structure-anchored trace chunker
/// when average row size is at least [`AUTO_ROW_THRESHOLD`] (long traces — edit
/// locality dominates), and to generic FastCDC otherwise (small rows — window
/// compression and low metadata dominate).
///
/// The sniff rule is part of the frozen boundary function: boundaries remain a
/// pure function of file content, so identical bytes always produce identical
/// chunks. Editing a file may flip its delegation only by changing the content
/// the rule measures — the benchmark's guardrail is that both delegates are
/// themselves stable, well-tested chunkers.
pub struct AutoTraceChunker;

impl Chunker for AutoTraceChunker {
    fn id(&self) -> ChunkerId {
        ChunkerId::TRACE_AUTO_V1
    }

    fn chunk<'r>(
        &self,
        mut reader: Box<dyn Read + Send + 'r>,
    ) -> Box<dyn Iterator<Item = Result<RawChunk, ChunkedError>> + Send + 'r> {
        // Buffer the sniff window, decide, then replay it ahead of the rest.
        let mut head = vec![0u8; AUTO_SNIFF_BYTES];
        let mut filled = 0usize;
        while filled < head.len() {
            match reader.read(&mut head[filled..]) {
                Ok(0) => break,
                Ok(n) => filled += n,
                Err(err) => {
                    return Box::new(std::iter::once(Err(ChunkedError::ChunkRead(err))));
                }
            }
        }
        head.truncate(filled);

        // Average row size over complete rows in the window. With no newline in
        // the window, rows are at least window-sized — long traces.
        let newlines = head.iter().filter(|&&b| b == b'\n').count();
        let measured = match head.iter().rposition(|&b| b == b'\n') {
            Some(last) => last + 1,
            None => head.len(),
        };
        let long_rows = newlines == 0 || measured / newlines >= AUTO_ROW_THRESHOLD;

        let replay: Box<dyn Read + Send + 'r> =
            Box::new(std::io::Cursor::new(head).chain(reader));
        if long_rows {
            TraceJsonlChunker.chunk(replay)
        } else {
            super::chunker::FastCdc2020Chunker.chunk(replay)
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::*;
    use crate::util::hasher::hash_buffer_128bit;

    fn chunk_all(data: &[u8]) -> Vec<RawChunk> {
        TraceJsonlChunker
            .chunk(Box::new(data))
            .collect::<Result<Vec<_>, _>>()
            .expect("chunking in-memory data cannot fail")
    }

    /// Chunks must tile the input exactly regardless of input shape.
    fn assert_tiles_input(chunks: &[RawChunk], data: &[u8]) {
        let mut expected_offset = 0u64;
        for (i, chunk) in chunks.iter().enumerate() {
            assert_eq!(chunk.offset, expected_offset, "chunk {i} offset");
            assert!(!chunk.data.is_empty(), "chunk {i} empty");
            assert!(
                chunk.data.len() <= MAX_CHUNK_SIZE as usize,
                "chunk {i} exceeds max size"
            );
            let start = chunk.offset as usize;
            assert_eq!(
                &data[start..start + chunk.data.len()],
                &chunk.data[..],
                "chunk {i} bytes"
            );
            expected_offset += chunk.data.len() as u64;
        }
        assert_eq!(
            expected_offset,
            data.len() as u64,
            "chunks must cover input"
        );
    }

    /// A deterministic chat-session row: `uuid`, a `messages` array with `count`
    /// ~500-byte entries, and a `source` column — the schema this chunker is built
    /// for. Rows with ≥10 messages span several intra-row targets.
    fn session_row(id: u64, count: usize) -> String {
        let messages: Vec<String> = (0..count)
            .map(|i| {
                let role = if i % 2 == 0 { "user" } else { "assistant" };
                format!(
                    r#"{{"role":"{role}","content":"message {i} of session {id}: {}"}}"#,
                    "lorem ipsum dolor sit amet ".repeat(40)
                )
            })
            .collect();
        format!(
            "{{\"uuid\":\"session-{id:08}\",\"messages\":[{}],\"source\":\"web\"}}\n",
            messages.join(",")
        )
    }

    fn chunk_hashes(chunks: &[RawChunk]) -> Vec<u128> {
        chunks.iter().map(|c| hash_buffer_128bit(&c.data)).collect()
    }

    /// Cut positions land only at row ends and depth-2 array element separators
    /// (plus the forced max-size cut), per the frozen cut rule.
    #[test]
    fn cuts_land_on_structural_anchors() {
        let data: String = (0..40).map(|i| session_row(i, 12)).collect();
        let chunks = chunk_all(data.as_bytes());
        assert_tiles_input(&chunks, data.as_bytes());
        assert!(chunks.len() > 40, "intra-row cuts expected");
        for chunk in &chunks[..chunks.len() - 1] {
            let last = *chunk.data.last().expect("chunks are non-empty");
            assert!(
                last == b'\n' || last == b',',
                "cut after unexpected byte {:?}",
                last as char
            );
        }
    }

    /// Rows over the isolation floor start at chunk boundaries, so an unchanged
    /// row yields identical chunks wherever it sits: reordering the file's rows
    /// changes no chunk identities at all.
    #[test]
    fn row_reorder_preserves_all_chunks() {
        let rows: Vec<String> = (0..30).map(|i| session_row(i, 10)).collect();
        let original: String = rows.iter().cloned().collect();
        let mut reordered_rows = rows.clone();
        reordered_rows.reverse();
        let reordered: String = reordered_rows.into_iter().collect();

        let original_hashes: HashSet<u128> = chunk_hashes(&chunk_all(original.as_bytes()))
            .into_iter()
            .collect();
        let reordered_chunks = chunk_all(reordered.as_bytes());
        assert_tiles_input(&reordered_chunks, reordered.as_bytes());
        for hash in chunk_hashes(&reordered_chunks) {
            assert!(
                original_hashes.contains(&hash),
                "reordering rows must not create new chunks"
            );
        }
    }

    /// Inserting a new row leaves every existing row's chunks intact — only the
    /// inserted row's own chunks are new.
    #[test]
    fn row_insertion_only_adds_the_new_rows_chunks() {
        let rows: Vec<String> = (0..30).map(|i| session_row(i, 10)).collect();
        let original: String = rows.iter().cloned().collect();
        let mut edited_rows = rows.clone();
        edited_rows.insert(15, session_row(999, 10));
        let edited: String = edited_rows.into_iter().collect();

        let original_hashes: HashSet<u128> = chunk_hashes(&chunk_all(original.as_bytes()))
            .into_iter()
            .collect();
        let inserted_alone: HashSet<u128> =
            chunk_hashes(&chunk_all(session_row(999, 10).as_bytes()))
                .into_iter()
                .collect();
        let edited_chunks = chunk_all(edited.as_bytes());
        assert_tiles_input(&edited_chunks, edited.as_bytes());
        for hash in chunk_hashes(&edited_chunks) {
            assert!(
                original_hashes.contains(&hash) || inserted_alone.contains(&hash),
                "insertion must not perturb other rows' chunks"
            );
        }
    }

    /// The flagship property: appending messages to a session's array keeps every
    /// chunk before the row's tail byte-identical. The new version's chunks are the
    /// old chunks minus at most one tail chunk per grown row, plus new content.
    #[test]
    fn appending_messages_costs_only_the_row_tail() {
        let mut rows: Vec<String> = (0..30).map(|i| session_row(i, 10)).collect();
        let original: String = rows.iter().cloned().collect();
        let original_hashes: HashSet<u128> = chunk_hashes(&chunk_all(original.as_bytes()))
            .into_iter()
            .collect();

        // Grow one mid-file session from 10 to 14 messages.
        rows[15] = session_row(15, 14);
        let grown_row_len = rows[15].len();
        let edited: String = rows.into_iter().collect();
        let edited_chunks = chunk_all(edited.as_bytes());
        assert_tiles_input(&edited_chunks, edited.as_bytes());

        let new_chunks: Vec<&RawChunk> = edited_chunks
            .iter()
            .filter(|c| !original_hashes.contains(&hash_buffer_128bit(&c.data)))
            .collect();
        // The grown row re-stores only its tail: the old tail chunk's span plus
        // the four appended messages — far less than re-storing the whole row.
        let new_bytes: usize = new_chunks.iter().map(|c| c.data.len()).sum();
        assert!(
            !new_chunks.is_empty() && new_bytes < grown_row_len,
            "append must cost less than re-storing the grown row ({new_bytes} vs {grown_row_len})"
        );
    }

    /// Rows under the isolation floor coalesce; a run of tiny rows becomes shared
    /// chunks cut at row boundaries once the floor is reached.
    #[test]
    fn tiny_rows_coalesce() {
        let data: String = (0..1000)
            .map(|i| format!("{{\"id\":{i},\"label\":\"x\"}}\n"))
            .collect();
        let chunks = chunk_all(data.as_bytes());
        assert_tiles_input(&chunks, data.as_bytes());
        for chunk in &chunks[..chunks.len() - 1] {
            assert!(chunk.data.len() >= ROW_ISOLATE_MIN);
            assert_eq!(*chunk.data.last().expect("non-empty"), b'\n');
        }
        // Far fewer chunks than rows: the floor is doing its job.
        assert!(chunks.len() < 100);
    }

    /// Arbitrary bytes with no structure degrade to forced max-size cuts — never a
    /// panic, never a malformed tiling.
    #[test]
    fn structureless_input_degrades_to_forced_cuts() {
        let mut state = 0x1234_5678_9ABC_DEF0u64;
        let data: Vec<u8> = std::iter::repeat_with(|| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            // Alphabetic bytes only: no newlines and no JSON structure, so no
            // anchors of either kind can fire.
            b'a' + (state % 26) as u8
        })
        .take(3 * MAX_CHUNK_SIZE as usize + 500)
        .collect();
        let chunks = chunk_all(&data);
        assert_tiles_input(&chunks, &data);
        let lens: Vec<usize> = chunks.iter().map(|c| c.data.len()).collect();
        assert_eq!(
            lens,
            vec![
                MAX_CHUNK_SIZE as usize,
                MAX_CHUNK_SIZE as usize,
                MAX_CHUNK_SIZE as usize,
                500
            ]
        );
    }

    /// A malformed line (unbalanced containers, unterminated string) cannot
    /// corrupt anchor detection for the lines after it: the scanner resets at
    /// every newline.
    #[test]
    fn malformed_line_does_not_poison_later_rows() {
        let good_row = session_row(7, 10);
        let malformed = "{\"broken\": [[[\"unterminated string...\n";
        let data = format!("{malformed}{good_row}");
        let clean_hashes: HashSet<u128> = chunk_hashes(&chunk_all(good_row.as_bytes()))
            .into_iter()
            .collect();

        let chunks = chunk_all(data.as_bytes());
        assert_tiles_input(&chunks, data.as_bytes());
        let shared = chunks
            .iter()
            .filter(|c| clean_hashes.contains(&hash_buffer_128bit(&c.data)))
            .count();
        // The good row's chunks reappear except at most the chunk polluted by the
        // (sub-floor) malformed line coalescing into it.
        assert!(
            shared >= clean_hashes.len() - 1,
            "expected the good row's chunks to survive a malformed predecessor"
        );
    }

    /// Small inputs and edge cases: empty input yields no chunks; input without a
    /// trailing newline still flushes; a lone tiny line is one chunk.
    #[test]
    fn small_inputs_and_missing_trailing_newline() {
        assert!(chunk_all(&[]).is_empty());

        let no_newline = b"{\"id\":1}";
        let chunks = chunk_all(no_newline);
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].data, no_newline);

        let one_row = session_row(1, 30);
        let chunks = chunk_all(one_row.as_bytes());
        assert_tiles_input(&chunks, one_row.as_bytes());
    }

    /// Commas inside strings, nested objects, and deep nesting are not anchors:
    /// only depth-2 array element separators cut.
    #[test]
    fn string_and_nested_commas_are_not_anchors() {
        // A row whose long content strings are full of commas and nested
        // structures; anchors must only land between messages.
        let messages: Vec<String> = (0..20)
            .map(|i| {
                format!(r#"{{"role":"tool","content":"a,b,c {i}","calls":[{{"deep":[1,2,3]}}]}}"#)
            })
            .collect();
        let row = format!(
            "{{\"uuid\":\"x\",\"messages\":[{}],\"source\":\"api\"}}\n",
            messages.join(",")
        );
        let chunks = chunk_all(row.as_bytes());
        assert_tiles_input(&chunks, row.as_bytes());
        for chunk in &chunks[..chunks.len() - 1] {
            let data = &chunk.data;
            // Every non-final cut must fall after `},` — the boundary between
            // elements of the depth-2 messages array (or a row end).
            assert!(
                data.ends_with(b"},") || data.ends_with(b"\n"),
                "cut inside a message or string: ...{:?}",
                String::from_utf8_lossy(&data[data.len().saturating_sub(12)..])
            );
        }
    }

    /// The auto chunker's delegation is a pure function of content: long-trace
    /// rows produce exactly the structural chunker's boundaries, small rows
    /// produce exactly generic FastCDC's, and both tile the input.
    #[test]
    fn auto_chunker_delegates_by_row_size() {
        let chunk_with = |c: &dyn Chunker, data: &[u8]| -> Vec<(u64, usize)> {
            c.chunk(Box::new(data.to_vec().as_slice() as &[u8]))
                .collect::<Result<Vec<_>, _>>()
                .expect("chunk in-memory data")
                .iter()
                .map(|c| (c.offset, c.data.len()))
                .collect()
        };

        // Long rows: 16 large messages per session (well over the threshold).
        let long: String = (0..20).map(|i| session_row(i, 16)).collect();
        let long_chunks = chunk_all(long.as_bytes());
        assert_tiles_input(&long_chunks, long.as_bytes());
        let expected: Vec<(u64, usize)> = long_chunks
            .iter()
            .map(|c| (c.offset, c.data.len()))
            .collect();
        let auto: Vec<(u64, usize)> = AutoTraceChunker
            .chunk(Box::new(long.as_bytes()))
            .collect::<Result<Vec<_>, _>>()
            .expect("auto chunk")
            .iter()
            .map(|c| (c.offset, c.data.len()))
            .collect();
        assert_eq!(auto, expected, "long rows must use the structural chunker");

        // Small rows: prompt/completion pairs a few hundred bytes each.
        let small: String = (0..8000)
            .map(|i| format!("{{\"prompt\":\"question {i} lorem ipsum dolor\",\"completion\":\"answer {i}\"}}\n"))
            .collect();
        let fastcdc = chunk_with(
            &crate::storage::chunked::chunker::FastCdc2020Chunker,
            small.as_bytes(),
        );
        let auto_small = chunk_with(&AutoTraceChunker, small.as_bytes());
        assert_eq!(auto_small, fastcdc, "small rows must use generic FastCDC");
    }

    /// Golden boundary fixture pinning TRACE_JSONL_V1's exact cut positions. If
    /// this fails, the boundary function changed — that must ship as a NEW chunker
    /// ID, never an in-place change (it would silently degrade dedup fleet-wide).
    #[test]
    fn golden_trace_boundaries() {
        let data: String = (0..8).map(|i| session_row(i, 16)).collect();
        let chunks = chunk_all(data.as_bytes());
        assert_tiles_input(&chunks, data.as_bytes());
        let boundaries: Vec<(u64, usize)> =
            chunks.iter().map(|c| (c.offset, c.data.len())).collect();
        // GOLDEN: (offset, len) pairs for eight 16-message sessions: one intra-row
        // cut at the first element separator past the target, then the row-end cut.
        let expected: Vec<(u64, usize)> = vec![
            (0, 9123),
            (9123, 9107),
            (18230, 9123),
            (27353, 9107),
            (36460, 9123),
            (45583, 9107),
            (54690, 9123),
            (63813, 9107),
            (72920, 9123),
            (82043, 9107),
            (91150, 9123),
            (100273, 9107),
            (109380, 9123),
            (118503, 9107),
            (127610, 9123),
            (136733, 9107),
        ];
        assert_eq!(boundaries, expected);
    }
}
