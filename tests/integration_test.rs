use ferrite_kv::aof::Aof;
use ferrite_kv::server::Server;
use ferrite_kv::storage::Storage;
use std::fs;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

#[tokio::test]
async fn test_end_to_end_server() {
    let db = Arc::new(Storage::new());
    let server = Server::new("127.0.0.1:16379", db, None).await.unwrap();

    tokio::spawn(async move {
        let _ = server.run().await;
    });

    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

    let mut stream = TcpStream::connect("127.0.0.1:16379").await.unwrap();

    // 1. Send PING
    stream.write_all(b"*1\r\n$4\r\nPING\r\n").await.unwrap();
    let mut buf = [0u8; 128];
    let n = stream.read(&mut buf).await.unwrap();
    assert_eq!(&buf[..n], b"+PONG\r\n");

    // 2. Send SET name rustacean
    stream
        .write_all(b"*3\r\n$3\r\nSET\r\n$4\r\nname\r\n$9\r\nrustacean\r\n")
        .await
        .unwrap();
    let n = stream.read(&mut buf).await.unwrap();
    assert_eq!(&buf[..n], b"+OK\r\n");

    // 3. Send GET name
    stream
        .write_all(b"*2\r\n$3\r\nGET\r\n$4\r\nname\r\n")
        .await
        .unwrap();
    let n = stream.read(&mut buf).await.unwrap();
    assert_eq!(&buf[..n], b"$9\r\nrustacean\r\n");

    // 4. Send INCR counter
    stream
        .write_all(b"*2\r\n$4\r\nINCR\r\n$7\r\ncounter\r\n")
        .await
        .unwrap();
    let n = stream.read(&mut buf).await.unwrap();
    assert_eq!(&buf[..n], b":1\r\n");

    // 5. Send DEL name
    stream
        .write_all(b"*2\r\n$3\r\nDEL\r\n$4\r\nname\r\n")
        .await
        .unwrap();
    let n = stream.read(&mut buf).await.unwrap();
    assert_eq!(&buf[..n], b":1\r\n");

    // 6. Send GET name (should return null bulk string $-1\r\n)
    stream
        .write_all(b"*2\r\n$3\r\nGET\r\n$4\r\nname\r\n")
        .await
        .unwrap();
    let n = stream.read(&mut buf).await.unwrap();
    assert_eq!(&buf[..n], b"$-1\r\n");
}

#[tokio::test]
async fn test_server_with_aof_persistence() {
    let temp_dir = std::env::temp_dir();
    let aof_path = temp_dir.join(format!(
        "ferrite_e2e_{}.aof",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));

    // Phase 1: Start Server 1 with AOF enabled
    {
        let db1 = Arc::new(Storage::new());
        let (aof_writer, _handle) = Aof::new(&aof_path);
        let server1 = Server::new("127.0.0.1:16380", db1, Some(Arc::new(aof_writer)))
            .await
            .unwrap();

        tokio::spawn(async move {
            let _ = server1.run().await;
        });

        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

        let mut stream = TcpStream::connect("127.0.0.1:16380").await.unwrap();
        // SET key val
        stream
            .write_all(b"*3\r\n$3\r\nSET\r\n$4\r\nhero\r\n$7\r\nferrite\r\n")
            .await
            .unwrap();
        let mut buf = [0u8; 128];
        let n = stream.read(&mut buf).await.unwrap();
        assert_eq!(&buf[..n], b"+OK\r\n");

        // INCR stars
        stream
            .write_all(b"*2\r\n$4\r\nINCR\r\n$5\r\nstars\r\n")
            .await
            .unwrap();
        let n = stream.read(&mut buf).await.unwrap();
        assert_eq!(&buf[..n], b":1\r\n");

        // Wait a brief moment for AOF writer task to flush
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
    }

    // Phase 2: Start Server 2, replaying AOF from disk
    {
        let db2 = Arc::new(Storage::new());
        let replayed = Aof::load_and_replay(&aof_path, &db2).await.unwrap();
        assert_eq!(replayed, 2);

        let server2 = Server::new("127.0.0.1:16381", db2, None).await.unwrap();
        tokio::spawn(async move {
            let _ = server2.run().await;
        });

        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

        let mut stream = TcpStream::connect("127.0.0.1:16381").await.unwrap();

        // GET hero
        stream
            .write_all(b"*2\r\n$3\r\nGET\r\n$4\r\nhero\r\n")
            .await
            .unwrap();
        let mut buf = [0u8; 128];
        let n = stream.read(&mut buf).await.unwrap();
        assert_eq!(&buf[..n], b"$7\r\nferrite\r\n");

        // GET stars
        stream
            .write_all(b"*2\r\n$3\r\nGET\r\n$5\r\nstars\r\n")
            .await
            .unwrap();
        let n = stream.read(&mut buf).await.unwrap();
        assert_eq!(&buf[..n], b"$1\r\n1\r\n");
    }

    let _ = fs::remove_file(&aof_path);
}
