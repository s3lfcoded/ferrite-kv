use bytes::BytesMut;
use ferrite_kv::resp::Frame;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let port = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "6389".to_string());
    let target = format!("127.0.0.1:{}", port);
    let total_requests = 100_000;
    let concurrency = 50;
    let requests_per_client = total_requests / concurrency;

    println!("==================================================");
    println!("FerriteKV High-Concurrency Benchmark");
    println!("Target:        {}", target);
    println!("Total ops:     {}", total_requests);
    println!("Concurrency:   {} clients", concurrency);
    println!("Ops/client:    {}", requests_per_client);
    println!("==================================================");

    // Warm-up / connectivity check
    let mut check_stream = match TcpStream::connect(&target).await {
        Ok(s) => s,
        Err(e) => {
            eprintln!(
                "Error: Cannot connect to {} (is the server running?): {}",
                target, e
            );
            eprintln!("Start the server first with: cargo run --release");
            return Ok(());
        }
    };
    check_stream.write_all(b"*1\r\n$4\r\nPING\r\n").await?;
    let mut buf = [0u8; 16];
    let _ = check_stream.read(&mut buf).await?;
    drop(check_stream);

    let completed_ops = Arc::new(AtomicUsize::new(0));
    let start = Instant::now();

    let mut handles = Vec::with_capacity(concurrency);

    for client_id in 0..concurrency {
        let completed = completed_ops.clone();
        let target = target.clone();
        handles.push(tokio::spawn(async move {
            let mut stream = match TcpStream::connect(&target).await {
                Ok(s) => s,
                Err(e) => {
                    eprintln!("Client {} failed to connect: {}", client_id, e);
                    return;
                }
            };

            let mut out = BytesMut::with_capacity(256);
            let mut in_buf = [0u8; 512];

            for i in 0..requests_per_client {
                out.clear();
                let key = format!("bench:c{}:k{}", client_id, i % 1000);
                let val = "ferrite_benchmark_payload_value";

                // Alternating SET and GET
                if i % 2 == 0 {
                    let frame = Frame::Array(vec![
                        Frame::Bulk(bytes::Bytes::from("SET")),
                        Frame::Bulk(bytes::Bytes::from(key)),
                        Frame::Bulk(bytes::Bytes::from(val)),
                    ]);
                    frame.serialize(&mut out);
                } else {
                    let frame = Frame::Array(vec![
                        Frame::Bulk(bytes::Bytes::from("GET")),
                        Frame::Bulk(bytes::Bytes::from(key)),
                    ]);
                    frame.serialize(&mut out);
                }

                if stream.write_all(&out).await.is_err() {
                    break;
                }
                if stream.read(&mut in_buf).await.is_err() {
                    break;
                }

                completed.fetch_add(1, Ordering::Relaxed);
            }
        }));
    }

    for handle in handles {
        let _ = handle.await;
    }

    let duration = start.elapsed();
    let total = completed_ops.load(Ordering::SeqCst);
    let rps = total as f64 / duration.as_secs_f64();

    println!("\nBenchmark Results:");
    println!("Completed Ops: {}", total);
    println!("Total Time:    {:.3?}", duration);
    println!("Throughput:    {:.2} ops/sec", rps);
    println!(
        "Avg Latency:   {:.3} ms",
        (duration.as_secs_f64() / total as f64) * 1000.0 * concurrency as f64
    );
    println!("==================================================");

    Ok(())
}
