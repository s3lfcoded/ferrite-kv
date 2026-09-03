use crate::command::Command;
use crate::resp::Frame;
use crate::storage::Storage;
use bytes::BytesMut;
use std::io::Cursor;
use std::path::Path;
use std::sync::Arc;
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufWriter};
use tokio::sync::mpsc::{self, Sender};
use tracing::{error, info, warn};

/// Handles asynchronous Append-Only File (AOF) durability.
pub struct Aof {
    sender: Sender<Frame>,
}

impl Aof {
    /// Starts the AOF writer task, returning the `Aof` handle.
    pub fn new<P: AsRef<Path>>(path: P) -> (Self, tokio::task::JoinHandle<()>) {
        let (tx, mut rx) = mpsc::channel::<Frame>(4096);
        let writer_path = path.as_ref().to_path_buf();
        let handle = tokio::spawn(async move {
            let file = match OpenOptions::new()
                .create(true)
                .append(true)
                .open(&writer_path)
                .await
            {
                Ok(f) => f,
                Err(err) => {
                    error!("Failed to open AOF file {:?}: {}", writer_path, err);
                    return;
                }
            };

            let mut writer = BufWriter::new(file);
            let mut out = BytesMut::with_capacity(4096);

            while let Some(frame) = rx.recv().await {
                out.clear();
                frame.serialize(&mut out);
                if let Err(e) = writer.write_all(&out).await {
                    error!("Error writing to AOF file: {}", e);
                }
                // Flush to ensure data durability
                if let Err(e) = writer.flush().await {
                    error!("Error flushing AOF file: {}", e);
                }
            }
        });

        (Self { sender: tx }, handle)
    }

    /// Appends a mutating command to the AOF queue asynchronously.
    pub async fn append(&self, frame: Frame) {
        if let Err(e) = self.sender.send(frame).await {
            warn!("Failed to queue command for AOF: {}", e);
        }
    }

    /// Replays the AOF log from disk on startup to reconstruct state.
    pub async fn load_and_replay<P: AsRef<Path>>(
        path: P,
        db: &Arc<Storage>,
    ) -> std::io::Result<usize> {
        let p = path.as_ref();
        if !p.exists() {
            return Ok(0);
        }

        let mut file = File::open(p).await?;
        let mut buffer = Vec::new();
        file.read_to_end(&mut buffer).await?;

        let mut parse_pos = 0;
        let mut count = 0;
        while parse_pos < buffer.len() {
            let mut check_cursor = Cursor::new(&buffer[parse_pos..]);
            if Frame::check(&mut check_cursor).is_err() {
                break;
            }
            let frame_len = check_cursor.position() as usize;
            let mut parse_cursor = Cursor::new(&buffer[parse_pos..parse_pos + frame_len]);
            if let Ok(frame) = Frame::parse(&mut parse_cursor)
                && let Ok(cmd) = Command::from_frame(frame)
            {
                cmd.execute(db);
                count += 1;
            }
            parse_pos += frame_len;
        }

        info!("Replayed {} commands from AOF log", count);
        Ok(count)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use std::fs;

    #[tokio::test]
    async fn test_aof_replay() {
        let temp_dir = std::env::temp_dir();
        let test_aof = temp_dir.join(format!(
            "test_{}.aof",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));

        let cmd1 = Command::Set {
            key: "hero".into(),
            value: Bytes::from("rustacean"),
            ttl: None,
        };
        let cmd2 = Command::Incr("visits".into());

        let mut out = BytesMut::new();
        cmd1.to_frame().unwrap().serialize(&mut out);
        cmd2.to_frame().unwrap().serialize(&mut out);

        fs::write(&test_aof, &out).unwrap();

        let db = Arc::new(Storage::new());
        let replayed = Aof::load_and_replay(&test_aof, &db).await.unwrap();
        assert_eq!(replayed, 2);

        assert_eq!(db.get("hero"), Some(Bytes::from("rustacean")));
        assert_eq!(db.get("visits"), Some(Bytes::from("1")));

        let _ = fs::remove_file(&test_aof);
    }
}
