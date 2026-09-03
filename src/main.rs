use ferrite_kv::aof::Aof;
use ferrite_kv::server::Server;
use ferrite_kv::storage::Storage;
use std::env;
use std::sync::Arc;
use tracing::info;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize structured logging
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    // Parse minimal CLI arguments without external heavy crates
    let args: Vec<String> = env::args().collect();
    let mut port: u16 = 6379;
    let mut aof_path: Option<String> = None;

    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "-p" | "--port" => {
                if i + 1 < args.len() {
                    port = args[i + 1].parse().unwrap_or(6379);
                    i += 2;
                } else {
                    i += 1;
                }
            }
            "--aof" => {
                if i + 1 < args.len() {
                    aof_path = Some(args[i + 1].clone());
                    i += 2;
                } else {
                    aof_path = Some("appendonly.aof".to_string());
                    i += 1;
                }
            }
            _ => {
                i += 1;
            }
        }
    }

    println!(
        r#"
   ______               _ __        __ ____   __
  / ____/__  __________(_) /____   / //_/\ \ / /
 / /_  / _ \/ ___/ ___/ / __/ _ \ / ,<    \ V / 
/ __/ /  __/ /  / /  / / /_/  __// /| |    | |  
/_/    \___/_/  /_/  /_/\__/\___//_/ |_|   |_|  
 High-Performance In-Memory Key-Value Store (RESP2)
"#
    );

    let db = Arc::new(Storage::new());

    // Setup AOF persistence if requested
    let aof = if let Some(path) = aof_path {
        info!("Loading AOF file from: {}", path);
        let _ = Aof::load_and_replay(&path, &db).await;
        let (aof_writer, _handle) = Aof::new(&path);
        Some(Arc::new(aof_writer))
    } else {
        None
    };

    let bind_addr = format!("127.0.0.1:{}", port);
    let server = Server::new(&bind_addr, db, aof).await?;

    tokio::select! {
        res = server.run() => {
            if let Err(err) = res {
                eprintln!("Server error: {}", err);
            }
        }
        _ = tokio::signal::ctrl_c() => {
            info!("Received shutdown signal, terminating cleanly...");
        }
    }

    Ok(())
}
