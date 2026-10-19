//! Incremental JSON frame reassembly for WebSocket text streams.

/// Result of feeding a chunk into [`JsonFrameBuffer`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BufferPush {
    /// The chunk was stored. No complete JSON value is available yet. More bytes are needed.
    Incomplete,
    /// One or more complete JSON values were cut out, in arrival order. Each string is the text of
    /// one object or array, without the whitespace around it.
    Complete(Vec<String>),
}

/// Puts the pieces of a JSON stream back together and cuts it into whole top-level values.
///
/// Some WebSocket servers split one JSON message over several text frames, or glue several
/// messages into one frame. Feed every received chunk to [`push`](Self::push). It keeps the
/// unfinished tail and returns each complete top-level object or array.
///
/// What it does and does not do:
///
/// - It counts brackets and understands strings and escapes, so a brace inside `"..."` does not
///   end a value.
/// - It does **not** check that the text is valid JSON. `{not json}` is returned as a complete
///   value. Parse each returned string yourself.
/// - Only values that start with `{` or `[` are recognised. If the pending text starts with
///   anything else (a bare number, a word, a stray `}`), nothing is ever returned and the text
///   stays in the buffer, growing with every chunk. Call [`reset`](Self::reset) when the stream
///   is reconnected or corrupt.
///
/// # Examples
///
/// ```
/// use brokapi_sansio::{BufferPush, JsonFrameBuffer};
///
/// let mut buf = JsonFrameBuffer::new();
///
/// // One message arrives in two pieces.
/// assert_eq!(buf.push(r#"{"op":"quote","bid":"#), BufferPush::Incomplete);
/// assert_eq!(
///     buf.push(r#"100}"#),
///     BufferPush::Complete(vec![r#"{"op":"quote","bid":100}"#.to_string()]),
/// );
///
/// // Two messages arrive glued together, and a third starts.
/// let got = buf.push(r#"{"a":1} {"b":"}"} {"c""#);
/// assert_eq!(
///     got,
///     BufferPush::Complete(vec![r#"{"a":1}"#.to_string(), r#"{"b":"}"}"#.to_string()]),
/// );
/// assert_eq!(buf.pending(), r#"{"c""#);
///
/// // The caller can drop a half-received message, for example after a reconnect.
/// buf.reset();
/// assert!(buf.is_empty());
///
/// // Text that cannot start a value blocks the buffer, even if a good value follows it.
/// assert_eq!(buf.push(r#"oops {"a":1}"#), BufferPush::Incomplete);
/// assert!(!buf.is_empty());
/// buf.reset();
/// ```
#[derive(Debug, Clone, Default)]
pub struct JsonFrameBuffer {
    buffer: String,
}

impl JsonFrameBuffer {
    /// Creates an empty buffer.
    pub fn new() -> Self {
        Self::default()
    }

    /// The text received but not yet returned: an unfinished value, or text that cannot start one.
    pub fn pending(&self) -> &str {
        &self.buffer
    }

    /// Whether nothing is pending. Whitespace-only input counts as nothing.
    pub fn is_empty(&self) -> bool {
        self.buffer.is_empty()
    }

    /// Appends a chunk and returns every complete top-level value that is now available.
    ///
    /// The returned strings are in arrival order. What follows them stays in the buffer. See the
    /// type docs for what is not checked.
    pub fn push(&mut self, chunk: &str) -> BufferPush {
        self.buffer.push_str(chunk);
        let mut frames = Vec::new();

        loop {
            let trimmed = self.buffer.trim_start();
            if trimmed.is_empty() {
                self.buffer.clear();
                break;
            }

            match extract_one_json(trimmed) {
                Some((frame, consumed)) => {
                    frames.push(frame);
                    self.buffer = trimmed[consumed..].to_string();
                }
                None => break,
            }
        }

        if frames.is_empty() {
            BufferPush::Incomplete
        } else {
            BufferPush::Complete(frames)
        }
    }

    /// Throws away everything pending.
    pub fn reset(&mut self) {
        self.buffer.clear();
    }
}

/// Returns `(json_text, byte_len)` when `input` starts with one complete JSON value.
fn extract_one_json(input: &str) -> Option<(String, usize)> {
    let bytes = input.as_bytes();
    let mut i = 0;
    while i < bytes.len() && bytes[i].is_ascii_whitespace() {
        i += 1;
    }
    if i >= bytes.len() {
        return None;
    }

    let start = i;
    let open = bytes[i];
    let (open_ch, close_ch) = match open {
        b'{' => (b'{', b'}'),
        b'[' => (b'[', b']'),
        _ => return None,
    };

    let mut depth = 0usize;
    let mut in_string = false;
    let mut escaped = false;

    while i < bytes.len() {
        let ch = bytes[i];
        if in_string {
            if escaped {
                escaped = false;
            } else if ch == b'\\' {
                escaped = true;
            } else if ch == b'"' {
                in_string = false;
            }
        } else {
            match ch {
                b'"' => in_string = true,
                c if c == open_ch => depth += 1,
                c if c == close_ch => {
                    depth = depth.saturating_sub(1);
                    if depth == 0 {
                        let end = i + 1;
                        let frame = input[start..end].to_string();
                        let consumed = end + (input[end..].len() - input[end..].trim_start().len());
                        return Some((frame, consumed));
                    }
                }
                _ => {}
            }
        }
        i += 1;
    }

    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn complete_object_in_one_chunk() {
        let mut buf = JsonFrameBuffer::new();
        match buf.push(r#"{"a":1}"#) {
            BufferPush::Complete(v) => assert_eq!(v, vec![r#"{"a":1}"#.to_string()]),
            other => panic!("expected complete, got {other:?}"),
        }
        assert!(buf.is_empty());
    }

    #[test]
    fn split_object_across_chunks() {
        let mut buf = JsonFrameBuffer::new();
        assert_eq!(buf.push(r#"{"a":"#), BufferPush::Incomplete);
        assert_eq!(
            buf.push(r#"1}"#),
            BufferPush::Complete(vec![r#"{"a":1}"#.to_string()])
        );
    }

    #[test]
    fn truncated_json_stays_buffered() {
        let mut buf = JsonFrameBuffer::new();
        assert_eq!(buf.push(r#"{"a":1"#), BufferPush::Incomplete);
        assert!(!buf.is_empty());
        assert_eq!(
            buf.push("}"),
            BufferPush::Complete(vec![r#"{"a":1}"#.to_string()])
        );
    }
}
