use crate::resp::{Frame, RespError};
use bytes::{Buf, BytesMut};
use std::fmt;
use std::io::Cursor;
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufWriter};
use tokio::net::TcpStream;

#[derive(Debug)]
pub enum ConnectionError {
    Io(std::io::Error),
    Protocol(RespError),
    ResetByPeer,
}

impl fmt::Display for ConnectionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ConnectionError::Io(err) => write!(f, "IO error: {}", err),
            ConnectionError::Protocol(err) => write!(f, "Protocol error: {}", err),
            ConnectionError::ResetByPeer => write!(f, "Connection reset by peer"),
        }
    }
}

impl std::error::Error for ConnectionError {}

impl From<std::io::Error> for ConnectionError {
    fn from(err: std::io::Error) -> Self {
        ConnectionError::Io(err)
    }
}

impl From<RespError> for ConnectionError {
    fn from(err: RespError) -> Self {
        ConnectionError::Protocol(err)
    }
}

/// Manages a buffered TCP connection to a client, reading and writing RESP frames.
pub struct Connection {
    stream: BufWriter<TcpStream>,
    buffer: BytesMut,
}

impl Connection {
    /// Creates a new `Connection` with a 4KB initial read buffer.
    pub fn new(stream: TcpStream) -> Self {
        Self {
            stream: BufWriter::new(stream),
            buffer: BytesMut::with_capacity(4096),
        }
    }

    /// Reads a single RESP frame from the socket.
    /// Returns `None` if the remote peer closed the connection.
    pub async fn read_frame(&mut self) -> Result<Option<Frame>, ConnectionError> {
        loop {
            // Attempt to parse a frame from buffered bytes.
            if let Some(frame) = self.parse_frame()? {
                return Ok(Some(frame));
            }

            // If incomplete or buffer is empty, read more bytes from the socket.
            if 0 == self.stream.read_buf(&mut self.buffer).await? {
                if self.buffer.is_empty() {
                    return Ok(None);
                } else {
                    return Err(ConnectionError::ResetByPeer);
                }
            }
        }
    }

    /// Attempts to parse a frame from the internal buffer.
    fn parse_frame(&mut self) -> Result<Option<Frame>, RespError> {
        let mut buf = Cursor::new(&self.buffer[..]);

        match Frame::check(&mut buf) {
            Ok(_) => {
                let len = buf.position() as usize;
                buf.set_position(0);
                let frame = Frame::parse(&mut buf)?;
                self.buffer.advance(len);
                Ok(Some(frame))
            }
            Err(RespError::Incomplete) => Ok(None),
            Err(e) => Err(e),
        }
    }

    /// Writes a single frame to the stream and flushes.
    pub async fn write_frame(&mut self, frame: &Frame) -> Result<(), ConnectionError> {
        let mut out = BytesMut::new();
        frame.serialize(&mut out);
        self.stream.write_all(&out).await?;
        self.stream.flush().await?;
        Ok(())
    }
}
