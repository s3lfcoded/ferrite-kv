use bytes::{Bytes, BytesMut};
use ferrite_kv::resp::Frame;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Instant;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let port = std::env::args().nth(1).unwrap_or_else(|| "6399".to_string());
    let target = format!("127.0.0.1:{}", port);

    let total_operations: usize = std::env::args()
        .nth(2)
        .and_then(|s| s.parse().ok())
        .unwrap_or(1_000_000);
    let concurrency: usize = std::env::args()
        .nth(3)
        .and_then(|s| s.parse().ok())
        .unwrap_or(100);
    let ops_per_client = total_operations / concurrency;

    println!("=============================================================");
    println!("        🔥 FERRITE-KV HARDCORE CHAOS & STRESS TEST 🔥       ");
    println!("=============================================================");
    println!("Target:               {}", target);
    println!("Total Operations:     {}", total_operations);
    println!("Concurrent Clients:   {} active TCP sockets", concurrency);
    println!("Ops per Client:       {}", ops_per_client);
    println!("Workload Profile:");
    println!("  - 30% Persistent SET");
    println!("  - 30% High-frequency TTL Storm (PX 100ms - 500ms)");
    println!("  - 20% Concurrent GET (reading alive & expiring keys)");
    println!("  - 20% Contention Atomic INCR on 4 shared hot keys");
    println!("=============================================================");

    // Connectivity & health check
    let mut probe = match TcpStream::connect(&target).await {
        Ok(s) => s,
        Err(e) => {
            eprintln!("Failed to connect to {}: {}", target, e);
            eprintln!("Make sure the server is running with --port {}", port);
            return Ok(());
        }
    };
    probe.write_all(b"*1\r\n$4\r\nPING\r\n").await?;
    let mut ping_buf = [0u8; 16];
    let _ = probe.read(&mut ping_buf).await?;
    drop(probe);
    println!("[✓] Connection probe successful. Launching 100 worker storm...\n");

    let success_count = Arc::new(AtomicUsize::new(0));
    let error_count = Arc::new(AtomicUsize::new(0));
    let start_time = Instant::now();

    let mut handles = Vec::with_capacity(concurrency);

    for client_id in 0..concurrency {
        let success = success_count.clone();
        let errors = error_count.clone();
        let target = target.clone();

        handles.push(tokio::spawn(async move {
            let mut stream = match TcpStream::connect(&target).await {
                Ok(s) => s,
                Err(_) => {
                    errors.fetch_add(ops_per_client, Ordering::Relaxed);
                    return;
                }
            };

            let mut out = BytesMut::with_capacity(512);
            let mut in_buf = [0u8; 1024];

            for i in 0..ops_per_client {
                out.clear();
                let op_type = i % 10;

                let frame = match op_type {
                    // 30% Persistent SET
                    0..=2 => {
                        let key = format!("hardcore:pers:c{}:{}", client_id, i % 500);
                        let val = "hardcore_payload_data_constant";
                        Frame::Array(vec![
                            Frame::Bulk(Bytes::from("SET")),
                            Frame::Bulk(Bytes::from(key)),
                            Frame::Bulk(Bytes::from(val)),
                        ])
                    }
                    // 30% TTL Storm: expires in 100ms - 300ms
                    3..=5 => {
                        let key = format!("hardcore:ttl:c{}:{}", client_id, i % 200);
                        let val = "temporary_ephemeral_token";
                        let px = 100 + (i % 200);
                        Frame::Array(vec![
                            Frame::Bulk(Bytes::from("SET")),
                            Frame::Bulk(Bytes::from(key)),
                            Frame::Bulk(Bytes::from(val)),
                            Frame::Bulk(Bytes::from("PX")),
                            Frame::Bulk(Bytes::from(px.to_string())),
                        ])
                    }
                    // 20% Concurrent GET
                    6..=7 => {
                        let key = if i % 2 == 0 {
                            format!("hardcore:pers:c{}:{}", client_id, i % 500)
                        } else {
                            format!("hardcore:ttl:c{}:{}", client_id, i % 200)
                        };
                        Frame::Array(vec![
                            Frame::Bulk(Bytes::from("GET")),
                            Frame::Bulk(Bytes::from(key)),
                        ])
                    }
                    // 20% Contention INCR on 4 shared hot keys
                    _ => {
                        let hot_key = format!("hot:counter:shard:{}", client_id % 4);
                        Frame::Array(vec![
                            Frame::Bulk(Bytes::from("INCR")),
                            Frame::Bulk(Bytes::from(hot_key)),
                        ])
                    }
                };

                frame.serialize(&mut out);

                if stream.write_all(&out).await.is_err() {
                    errors.fetch_add(1, Ordering::Relaxed);
                    continue;
                }

                match stream.read(&mut in_buf).await {
                    Ok(n) if n > 0 => {
                        success.fetch_add(1, Ordering::Relaxed);
                    }
                    _ => {
                        errors.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
        }));
    }

    let prog_success = success_count.clone();
    let prog_total = total_operations;
    let progress_reporter = tokio::spawn(async move {
        let mut last = 0;
        loop {
            tokio::time::sleep(std::time::Duration::from_millis(1000)).await;
            let cur = prog_success.load(Ordering::Relaxed);
            if cur >= prog_total {
                break;
            }
            if cur > last {
                let delta = cur - last;
                println!(
                    "  [Progress] {:>7} / {} ops ({:.1}%) | Current Speed: {:>6} ops/sec",
                    cur,
                    prog_total,
                    (cur as f64 / prog_total as f64) * 100.0,
                    delta
                );
                last = cur;
            }
        }
    });

    for handle in handles {
        let _ = handle.await;
    }
    progress_reporter.abort();

    let elapsed = start_time.elapsed();
    let total_done = success_count.load(Ordering::SeqCst);
    let total_err = error_count.load(Ordering::SeqCst);
    let rps = total_done as f64 / elapsed.as_secs_f64();

    println!("=============================================================");
    println!("                    STRESS TEST COMPLETED                    ");
    println!("=============================================================");
    println!("Completed Operations: {}", total_done);
    println!("Failed / Dropped Ops: {}", total_err);
    println!("Total Elapsed Time:   {:.3?}", elapsed);
    println!("Sustained Throughput: {:.2} ops/sec", rps);
    println!("Average Round-Trip:   {:.3} ms", (elapsed.as_secs_f64() / total_done as f64) * 1000.0 * concurrency as f64);
    println!("=============================================================");

    Ok(())
}
