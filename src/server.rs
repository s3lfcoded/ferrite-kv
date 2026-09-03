use crate::aof::Aof;
use crate::command::Command;
use crate::connection::Connection;
use crate::resp::Frame;
use crate::storage::Storage;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::{TcpListener, TcpStream};
use tracing::{info, trace};

pub struct Server {
    listener: TcpListener,
    db: Arc<Storage>,
    aof: Option<Arc<Aof>>,
}

impl Server {
    /// Creates and binds a new server to `addr`.
    pub async fn new(addr: &str, db: Arc<Storage>, aof: Option<Arc<Aof>>) -> std::io::Result<Self> {
        let listener = TcpListener::bind(addr).await?;
        info!("FerriteKV listening on {}", addr);
        Ok(Self { listener, db, aof })
    }

    /// Runs the server accept loop.
    pub async fn run(self) -> std::io::Result<()> {
        // Spawn active TTL purge worker
        let db_clone = self.db.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_millis(250));
            loop {
                interval.tick().await;
                let purged = db_clone.purge_expired();
                if purged > 0 {
                    trace!("Purged {} expired keys in background", purged);
                }
            }
        });

        loop {
            let (stream, peer_addr) = self.listener.accept().await?;
            let db = self.db.clone();
            let aof = self.aof.clone();

            tokio::spawn(async move {
                if let Err(err) = handle_connection(stream, peer_addr, db, aof).await {
                    trace!("Connection from {} closed: {}", peer_addr, err);
                }
            });
        }
    }
}

async fn handle_connection(
    stream: TcpStream,
    peer_addr: SocketAddr,
    db: Arc<Storage>,
    aof: Option<Arc<Aof>>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut connection = Connection::new(stream);
    trace!("Accepted connection from {}", peer_addr);

    while let Some(frame) = connection.read_frame().await? {
        let command = match Command::from_frame(frame) {
            Ok(cmd) => cmd,
            Err(err) => {
                connection.write_frame(&Frame::Error(err)).await?;
                continue;
            }
        };

        // If mutating command and AOF is enabled, log to AOF
        if command.is_mutating()
            && let Some(ref aof_writer) = aof
            && let Some(cmd_frame) = command.to_frame()
        {
            aof_writer.append(cmd_frame).await;
        }

        let response = command.execute(&db);
        connection.write_frame(&response).await?;
    }

    Ok(())
}
