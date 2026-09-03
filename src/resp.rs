use bytes::{Buf, Bytes, BytesMut};
use std::fmt;
use std::io::Cursor;

/// Represents a RESP (REdis Serialization Protocol) frame.
#[derive(Debug, Clone, PartialEq)]
pub enum Frame {
    Simple(String),
    Error(String),
    Integer(i64),
    Bulk(Bytes),
    Null,
    Array(Vec<Frame>),
}

#[derive(Debug, PartialEq, Eq)]
pub enum RespError {
    /// The buffer does not yet contain a full frame.
    Incomplete,
    /// The protocol data is corrupted or invalid.
    Invalid(String),
}

impl fmt::Display for RespError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RespError::Incomplete => write!(f, "incomplete RESP frame"),
            RespError::Invalid(msg) => write!(f, "invalid RESP frame: {}", msg),
        }
    }
}

impl std::error::Error for RespError {}

impl Frame {
    /// Checks if a complete frame can be parsed from `src`.
    /// Advances `src` cursor if successful.
    pub fn check(src: &mut Cursor<&[u8]>) -> Result<(), RespError> {
        if !src.has_remaining() {
            return Err(RespError::Incomplete);
        }

        let first = src.get_u8();
        match first {
            b'+' | b'-' => {
                get_line(src)?;
                Ok(())
            }
            b':' => {
                get_decimal(src)?;
                Ok(())
            }
            b'$' => {
                if !src.has_remaining() {
                    return Err(RespError::Incomplete);
                }
                let len = get_decimal(src)?;
                if len >= 0 {
                    let len = len as usize;
                    // Bulk string must have len bytes + \r\n
                    if src.remaining() < len + 2 {
                        return Err(RespError::Incomplete);
                    }
                    src.advance(len);
                    if src.get_u8() != b'\r' || src.get_u8() != b'\n' {
                        return Err(RespError::Invalid("expected CRLF after bulk string".into()));
                    }
                }
                Ok(())
            }
            b'*' => {
                let count = get_decimal(src)?;
                if count > 0 {
                    for _ in 0..count {
                        Frame::check(src)?;
                    }
                }
                Ok(())
            }
            // Inline command check: e.g. "PING\r\n"
            _ => {
                src.set_position(src.position() - 1);
                get_line(src)?;
                Ok(())
            }
        }
    }

    /// Parses a frame from a `Cursor<&[u8]>`.
    /// Caller MUST ensure `Frame::check` succeeded or handle `RespError::Incomplete`.
    pub fn parse(src: &mut Cursor<&[u8]>) -> Result<Frame, RespError> {
        if !src.has_remaining() {
            return Err(RespError::Incomplete);
        }

        let first = src.get_u8();
        match first {
            b'+' => {
                let line = get_line(src)?;
                let string = String::from_utf8(line.to_vec())
                    .map_err(|_| RespError::Invalid("invalid UTF-8 in simple string".into()))?;
                Ok(Frame::Simple(string))
            }
            b'-' => {
                let line = get_line(src)?;
                let string = String::from_utf8(line.to_vec())
                    .map_err(|_| RespError::Invalid("invalid UTF-8 in error string".into()))?;
                Ok(Frame::Error(string))
            }
            b':' => {
                let val = get_decimal(src)?;
                Ok(Frame::Integer(val))
            }
            b'$' => {
                let len = get_decimal(src)?;
                if len < 0 {
                    Ok(Frame::Null)
                } else {
                    let len = len as usize;
                    let pos = src.position() as usize;
                    let slice: &[u8] = src.get_ref();
                    if slice.len() < pos + len + 2 {
                        return Err(RespError::Incomplete);
                    }
                    let data = Bytes::copy_from_slice(&slice[pos..pos + len]);
                    src.advance(len);
                    if src.get_u8() != b'\r' || src.get_u8() != b'\n' {
                        return Err(RespError::Invalid("expected CRLF after bulk string".into()));
                    }
                    Ok(Frame::Bulk(data))
                }
            }
            b'*' => {
                let count = get_decimal(src)?;
                if count < 0 {
                    Ok(Frame::Null)
                } else {
                    let mut frames = Vec::with_capacity(count as usize);
                    for _ in 0..count {
                        frames.push(Frame::parse(src)?);
                    }
                    Ok(Frame::Array(frames))
                }
            }
            // Inline command parsing (useful for telnet or netcat testing)
            _ => {
                src.set_position(src.position() - 1);
                let line = get_line(src)?;
                let line_str = std::str::from_utf8(line)
                    .map_err(|_| RespError::Invalid("invalid UTF-8 in inline command".into()))?;
                let parts: Vec<Frame> = line_str
                    .split_whitespace()
                    .map(|part| Frame::Bulk(Bytes::copy_from_slice(part.as_bytes())))
                    .collect();
                Ok(Frame::Array(parts))
            }
        }
    }

    /// Serializes this frame into RESP format and appends to `dst`.
    pub fn serialize(&self, dst: &mut BytesMut) {
        match self {
            Frame::Simple(s) => {
                dst.extend_from_slice(b"+");
                dst.extend_from_slice(s.as_bytes());
                dst.extend_from_slice(b"\r\n");
            }
            Frame::Error(s) => {
                dst.extend_from_slice(b"-");
                dst.extend_from_slice(s.as_bytes());
                dst.extend_from_slice(b"\r\n");
            }
            Frame::Integer(val) => {
                dst.extend_from_slice(b":");
                dst.extend_from_slice(val.to_string().as_bytes());
                dst.extend_from_slice(b"\r\n");
            }
            Frame::Bulk(data) => {
                dst.extend_from_slice(b"$");
                dst.extend_from_slice(data.len().to_string().as_bytes());
                dst.extend_from_slice(b"\r\n");
                dst.extend_from_slice(data);
                dst.extend_from_slice(b"\r\n");
            }
            Frame::Null => {
                dst.extend_from_slice(b"$-1\r\n");
            }
            Frame::Array(frames) => {
                dst.extend_from_slice(b"*");
                dst.extend_from_slice(frames.len().to_string().as_bytes());
                dst.extend_from_slice(b"\r\n");
                for frame in frames {
                    frame.serialize(dst);
                }
            }
        }
    }
}

/// Reads until the next `\r\n` delimiter without advancing beyond it, returning the slice before `\r\n`.
fn get_line<'a>(src: &mut Cursor<&'a [u8]>) -> Result<&'a [u8], RespError> {
    let start = src.position() as usize;
    let slice: &'a [u8] = src.get_ref();

    for i in start..slice.len() {
        if slice[i] == b'\r' {
            if i + 1 >= slice.len() {
                return Err(RespError::Incomplete);
            }
            if slice[i + 1] == b'\n' {
                src.set_position((i + 2) as u64);
                return Ok(&slice[start..i]);
            }
        }
    }

    Err(RespError::Incomplete)
}

/// Parses an ASCII decimal integer ending with `\r\n`.
fn get_decimal(src: &mut Cursor<&[u8]>) -> Result<i64, RespError> {
    let line = get_line(src)?;
    let s = std::str::from_utf8(line)
        .map_err(|_| RespError::Invalid("invalid UTF-8 decimal".into()))?;
    s.parse::<i64>()
        .map_err(|_| RespError::Invalid("failed to parse integer".into()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_simple_string() {
        let buf = b"+OK\r\n";
        let mut cursor = Cursor::new(&buf[..]);
        Frame::check(&mut cursor).unwrap();
        cursor.set_position(0);
        let frame = Frame::parse(&mut cursor).unwrap();
        assert_eq!(frame, Frame::Simple("OK".to_string()));
    }

    #[test]
    fn test_parse_error() {
        let buf = b"-ERR unknown command\r\n";
        let mut cursor = Cursor::new(&buf[..]);
        Frame::check(&mut cursor).unwrap();
        cursor.set_position(0);
        let frame = Frame::parse(&mut cursor).unwrap();
        assert_eq!(frame, Frame::Error("ERR unknown command".to_string()));
    }

    #[test]
    fn test_parse_integer() {
        let buf = b":1048576\r\n";
        let mut cursor = Cursor::new(&buf[..]);
        Frame::check(&mut cursor).unwrap();
        cursor.set_position(0);
        let frame = Frame::parse(&mut cursor).unwrap();
        assert_eq!(frame, Frame::Integer(1048576));
    }

    #[test]
    fn test_parse_bulk_string() {
        let buf = b"$5\r\nhello\r\n";
        let mut cursor = Cursor::new(&buf[..]);
        Frame::check(&mut cursor).unwrap();
        cursor.set_position(0);
        let frame = Frame::parse(&mut cursor).unwrap();
        assert_eq!(frame, Frame::Bulk(Bytes::from("hello")));
    }

    #[test]
    fn test_parse_null_bulk_string() {
        let buf = b"$-1\r\n";
        let mut cursor = Cursor::new(&buf[..]);
        Frame::check(&mut cursor).unwrap();
        cursor.set_position(0);
        let frame = Frame::parse(&mut cursor).unwrap();
        assert_eq!(frame, Frame::Null);
    }

    #[test]
    fn test_parse_array() {
        let buf = b"*2\r\n$3\r\nGET\r\n$4\r\nuser\r\n";
        let mut cursor = Cursor::new(&buf[..]);
        Frame::check(&mut cursor).unwrap();
        cursor.set_position(0);
        let frame = Frame::parse(&mut cursor).unwrap();
        assert_eq!(
            frame,
            Frame::Array(vec![
                Frame::Bulk(Bytes::from("GET")),
                Frame::Bulk(Bytes::from("user")),
            ])
        );
    }

    #[test]
    fn test_parse_inline() {
        let buf = b"PING\r\n";
        let mut cursor = Cursor::new(&buf[..]);
        Frame::check(&mut cursor).unwrap();
        cursor.set_position(0);
        let frame = Frame::parse(&mut cursor).unwrap();
        assert_eq!(frame, Frame::Array(vec![Frame::Bulk(Bytes::from("PING"))]));
    }

    #[test]
    fn test_incomplete_detection() {
        let buf = b"*2\r\n$3\r\nGET\r\n$4\r\nus";
        let mut cursor = Cursor::new(&buf[..]);
        assert_eq!(Frame::check(&mut cursor), Err(RespError::Incomplete));
    }

    #[test]
    fn test_serialize_and_parse_roundtrip() {
        let original = Frame::Array(vec![
            Frame::Bulk(Bytes::from("SET")),
            Frame::Bulk(Bytes::from("counter")),
            Frame::Bulk(Bytes::from("42")),
        ]);
        let mut out = BytesMut::new();
        original.serialize(&mut out);

        let mut cursor = Cursor::new(&out[..]);
        Frame::check(&mut cursor).unwrap();
        cursor.set_position(0);
        let parsed = Frame::parse(&mut cursor).unwrap();
        assert_eq!(original, parsed);
    }
}
