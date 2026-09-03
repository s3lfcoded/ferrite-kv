use crate::resp::Frame;
use crate::storage::Storage;
use bytes::Bytes;
use std::sync::Arc;
use std::time::Duration;

#[derive(Debug, PartialEq, Clone)]
pub enum Command {
    Ping(Option<Bytes>),
    Echo(Bytes),
    Get(String),
    Set {
        key: String,
        value: Bytes,
        ttl: Option<Duration>,
    },
    Del(Vec<String>),
    Exists(Vec<String>),
    Expire {
        key: String,
        ttl: Duration,
    },
    Ttl(String),
    Incr(String),
    Decr(String),
    IncrBy {
        key: String,
        delta: i64,
    },
    DecrBy {
        key: String,
        delta: i64,
    },
    MGet(Vec<String>),
    MSet(Vec<(String, Bytes)>),
    DbSize,
    FlushDb,
    Info,
    Command,
    Unknown(String),
}

impl Command {
    /// Parses a `Frame` (which must be an Array or inline command) into a typed `Command`.
    pub fn from_frame(frame: Frame) -> Result<Command, String> {
        let items = match frame {
            Frame::Array(items) => items,
            _ => return Err("ERR expected array of bulk strings".into()),
        };

        if items.is_empty() {
            return Err("ERR empty command".into());
        }

        let cmd_name = match &items[0] {
            Frame::Bulk(b) => std::str::from_utf8(b)
                .map_err(|_| "ERR invalid command name")?
                .to_ascii_uppercase(),
            Frame::Simple(s) => s.to_ascii_uppercase(),
            _ => return Err("ERR invalid command format".into()),
        };

        match cmd_name.as_str() {
            "PING" => {
                if items.len() > 2 {
                    return Err("ERR wrong number of arguments for 'ping' command".into());
                }
                let msg = if items.len() == 2 {
                    match &items[1] {
                        Frame::Bulk(b) => Some(b.clone()),
                        _ => None,
                    }
                } else {
                    None
                };
                Ok(Command::Ping(msg))
            }
            "ECHO" => {
                if items.len() != 2 {
                    return Err("ERR wrong number of arguments for 'echo' command".into());
                }
                match &items[1] {
                    Frame::Bulk(b) => Ok(Command::Echo(b.clone())),
                    _ => Err("ERR invalid argument for 'echo'".into()),
                }
            }
            "GET" => {
                if items.len() != 2 {
                    return Err("ERR wrong number of arguments for 'get' command".into());
                }
                let key = extract_string(&items[1])?;
                Ok(Command::Get(key))
            }
            "SET" => {
                if items.len() < 3 {
                    return Err("ERR wrong number of arguments for 'set' command".into());
                }
                let key = extract_string(&items[1])?;
                let value = match &items[2] {
                    Frame::Bulk(b) => b.clone(),
                    _ => return Err("ERR invalid value for 'set'".into()),
                };

                let mut ttl = None;
                let mut idx = 3;
                while idx < items.len() {
                    let opt = extract_string(&items[idx])?.to_ascii_uppercase();
                    if opt == "EX" {
                        if idx + 1 >= items.len() {
                            return Err("ERR syntax error".into());
                        }
                        let secs: u64 = extract_string(&items[idx + 1])?
                            .parse()
                            .map_err(|_| "ERR value is not an integer or out of range")?;
                        ttl = Some(Duration::from_secs(secs));
                        idx += 2;
                    } else if opt == "PX" {
                        if idx + 1 >= items.len() {
                            return Err("ERR syntax error".into());
                        }
                        let millis: u64 = extract_string(&items[idx + 1])?
                            .parse()
                            .map_err(|_| "ERR value is not an integer or out of range")?;
                        ttl = Some(Duration::from_millis(millis));
                        idx += 2;
                    } else {
                        return Err(format!("ERR syntax error unknown option '{}'", opt));
                    }
                }

                Ok(Command::Set { key, value, ttl })
            }
            "DEL" => {
                if items.len() < 2 {
                    return Err("ERR wrong number of arguments for 'del' command".into());
                }
                let mut keys = Vec::with_capacity(items.len() - 1);
                for item in &items[1..] {
                    keys.push(extract_string(item)?);
                }
                Ok(Command::Del(keys))
            }
            "EXISTS" => {
                if items.len() < 2 {
                    return Err("ERR wrong number of arguments for 'exists' command".into());
                }
                let mut keys = Vec::with_capacity(items.len() - 1);
                for item in &items[1..] {
                    keys.push(extract_string(item)?);
                }
                Ok(Command::Exists(keys))
            }
            "EXPIRE" => {
                if items.len() != 3 {
                    return Err("ERR wrong number of arguments for 'expire' command".into());
                }
                let key = extract_string(&items[1])?;
                let secs: u64 = extract_string(&items[2])?
                    .parse()
                    .map_err(|_| "ERR value is not an integer or out of range")?;
                Ok(Command::Expire {
                    key,
                    ttl: Duration::from_secs(secs),
                })
            }
            "TTL" => {
                if items.len() != 2 {
                    return Err("ERR wrong number of arguments for 'ttl' command".into());
                }
                let key = extract_string(&items[1])?;
                Ok(Command::Ttl(key))
            }
            "INCR" => {
                if items.len() != 2 {
                    return Err("ERR wrong number of arguments for 'incr' command".into());
                }
                let key = extract_string(&items[1])?;
                Ok(Command::Incr(key))
            }
            "DECR" => {
                if items.len() != 2 {
                    return Err("ERR wrong number of arguments for 'decr' command".into());
                }
                let key = extract_string(&items[1])?;
                Ok(Command::Decr(key))
            }
            "INCRBY" => {
                if items.len() != 3 {
                    return Err("ERR wrong number of arguments for 'incrby' command".into());
                }
                let key = extract_string(&items[1])?;
                let delta: i64 = extract_string(&items[2])?
                    .parse()
                    .map_err(|_| "ERR value is not an integer or out of range")?;
                Ok(Command::IncrBy { key, delta })
            }
            "DECRBY" => {
                if items.len() != 3 {
                    return Err("ERR wrong number of arguments for 'decrby' command".into());
                }
                let key = extract_string(&items[1])?;
                let delta: i64 = extract_string(&items[2])?
                    .parse()
                    .map_err(|_| "ERR value is not an integer or out of range")?;
                Ok(Command::DecrBy { key, delta })
            }
            "MGET" => {
                if items.len() < 2 {
                    return Err("ERR wrong number of arguments for 'mget' command".into());
                }
                let mut keys = Vec::with_capacity(items.len() - 1);
                for item in &items[1..] {
                    keys.push(extract_string(item)?);
                }
                Ok(Command::MGet(keys))
            }
            "MSET" => {
                if items.len() < 3 || (items.len() - 1) % 2 != 0 {
                    return Err("ERR wrong number of arguments for 'mset' command".into());
                }
                let mut pairs = Vec::with_capacity((items.len() - 1) / 2);
                let mut i = 1;
                while i < items.len() {
                    let k = extract_string(&items[i])?;
                    let v = match &items[i + 1] {
                        Frame::Bulk(b) => b.clone(),
                        _ => return Err("ERR invalid value in mset".into()),
                    };
                    pairs.push((k, v));
                    i += 2;
                }
                Ok(Command::MSet(pairs))
            }
            "DBSIZE" => Ok(Command::DbSize),
            "FLUSHDB" => Ok(Command::FlushDb),
            "INFO" => Ok(Command::Info),
            "COMMAND" => Ok(Command::Command),
            _ => Ok(Command::Unknown(cmd_name)),
        }
    }

    /// Checks if this command mutates state (and thus needs to be written to AOF log).
    pub fn is_mutating(&self) -> bool {
        matches!(
            self,
            Command::Set { .. }
                | Command::Del(_)
                | Command::Expire { .. }
                | Command::Incr(_)
                | Command::Decr(_)
                | Command::IncrBy { .. }
                | Command::DecrBy { .. }
                | Command::MSet(_)
                | Command::FlushDb
        )
    }

    /// Converts this command back into a canonical RESP Array Frame (for AOF serialization).
    pub fn to_frame(&self) -> Option<Frame> {
        match self {
            Command::Set { key, value, ttl } => {
                let mut parts = vec![
                    Frame::Bulk(Bytes::from("SET")),
                    Frame::Bulk(Bytes::from(key.clone())),
                    Frame::Bulk(value.clone()),
                ];
                if let Some(ttl) = ttl {
                    parts.push(Frame::Bulk(Bytes::from("EX")));
                    parts.push(Frame::Bulk(Bytes::from(ttl.as_secs().to_string())));
                }
                Some(Frame::Array(parts))
            }
            Command::Del(keys) => {
                let mut parts = vec![Frame::Bulk(Bytes::from("DEL"))];
                for k in keys {
                    parts.push(Frame::Bulk(Bytes::from(k.clone())));
                }
                Some(Frame::Array(parts))
            }
            Command::Expire { key, ttl } => Some(Frame::Array(vec![
                Frame::Bulk(Bytes::from("EXPIRE")),
                Frame::Bulk(Bytes::from(key.clone())),
                Frame::Bulk(Bytes::from(ttl.as_secs().to_string())),
            ])),
            Command::Incr(key) => Some(Frame::Array(vec![
                Frame::Bulk(Bytes::from("INCR")),
                Frame::Bulk(Bytes::from(key.clone())),
            ])),
            Command::Decr(key) => Some(Frame::Array(vec![
                Frame::Bulk(Bytes::from("DECR")),
                Frame::Bulk(Bytes::from(key.clone())),
            ])),
            Command::IncrBy { key, delta } => Some(Frame::Array(vec![
                Frame::Bulk(Bytes::from("INCRBY")),
                Frame::Bulk(Bytes::from(key.clone())),
                Frame::Bulk(Bytes::from(delta.to_string())),
            ])),
            Command::DecrBy { key, delta } => Some(Frame::Array(vec![
                Frame::Bulk(Bytes::from("DECRBY")),
                Frame::Bulk(Bytes::from(key.clone())),
                Frame::Bulk(Bytes::from(delta.to_string())),
            ])),
            Command::MSet(pairs) => {
                let mut parts = vec![Frame::Bulk(Bytes::from("MSET"))];
                for (k, v) in pairs {
                    parts.push(Frame::Bulk(Bytes::from(k.clone())));
                    parts.push(Frame::Bulk(v.clone()));
                }
                Some(Frame::Array(parts))
            }
            Command::FlushDb => Some(Frame::Array(vec![Frame::Bulk(Bytes::from("FLUSHDB"))])),
            _ => None,
        }
    }

    /// Executes the command against the provided `Storage` engine and returns the response `Frame`.
    pub fn execute(self, db: &Arc<Storage>) -> Frame {
        match self {
            Command::Ping(msg) => match msg {
                Some(m) => Frame::Bulk(m),
                None => Frame::Simple("PONG".into()),
            },
            Command::Echo(msg) => Frame::Bulk(msg),
            Command::Get(key) => match db.get(&key) {
                Some(val) => Frame::Bulk(val),
                None => Frame::Null,
            },
            Command::Set { key, value, ttl } => {
                db.set(key, value, ttl);
                Frame::Simple("OK".into())
            }
            Command::Del(keys) => {
                let count = db.del(&keys);
                Frame::Integer(count as i64)
            }
            Command::Exists(keys) => {
                let count = db.exists(&keys);
                Frame::Integer(count as i64)
            }
            Command::Expire { key, ttl } => {
                let set = db.expire(&key, ttl);
                Frame::Integer(if set { 1 } else { 0 })
            }
            Command::Ttl(key) => {
                let ttl = db.ttl(&key);
                Frame::Integer(ttl)
            }
            Command::Incr(key) => match db.incr_by(key, 1) {
                Ok(val) => Frame::Integer(val),
                Err(err) => Frame::Error(err),
            },
            Command::Decr(key) => match db.incr_by(key, -1) {
                Ok(val) => Frame::Integer(val),
                Err(err) => Frame::Error(err),
            },
            Command::IncrBy { key, delta } => match db.incr_by(key, delta) {
                Ok(val) => Frame::Integer(val),
                Err(err) => Frame::Error(err),
            },
            Command::DecrBy { key, delta } => match db.incr_by(key, -delta) {
                Ok(val) => Frame::Integer(val),
                Err(err) => Frame::Error(err),
            },
            Command::MGet(keys) => {
                let mut results = Vec::with_capacity(keys.len());
                for key in keys {
                    match db.get(&key) {
                        Some(val) => results.push(Frame::Bulk(val)),
                        None => results.push(Frame::Null),
                    }
                }
                Frame::Array(results)
            }
            Command::MSet(pairs) => {
                for (key, val) in pairs {
                    db.set(key, val, None);
                }
                Frame::Simple("OK".into())
            }
            Command::DbSize => {
                let size = db.dbsize();
                Frame::Integer(size as i64)
            }
            Command::FlushDb => {
                db.flushdb();
                Frame::Simple("OK".into())
            }
            Command::Info => {
                let info = format!(
                    "# Server\r\n\
                     ferrite_version:0.1.0\r\n\
                     tcp_port:6379\r\n\
                     # Keyspace\r\n\
                     keys={}\r\n",
                    db.dbsize()
                );
                Frame::Bulk(Bytes::from(info))
            }
            Command::Command => {
                // Return empty array for client capability handshake
                Frame::Array(vec![])
            }
            Command::Unknown(cmd) => Frame::Error(format!("ERR unknown command '{}'", cmd)),
        }
    }
}

fn extract_string(frame: &Frame) -> Result<String, String> {
    match frame {
        Frame::Bulk(b) => std::str::from_utf8(b)
            .map(|s| s.to_string())
            .map_err(|_| "ERR invalid UTF-8 string".into()),
        Frame::Simple(s) => Ok(s.clone()),
        _ => Err("ERR expected string argument".into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_ping_and_get_set() {
        let ping_frame = Frame::Array(vec![Frame::Bulk(Bytes::from("PING"))]);
        let cmd = Command::from_frame(ping_frame).unwrap();
        assert_eq!(cmd, Command::Ping(None));

        let set_frame = Frame::Array(vec![
            Frame::Bulk(Bytes::from("SET")),
            Frame::Bulk(Bytes::from("mykey")),
            Frame::Bulk(Bytes::from("myval")),
            Frame::Bulk(Bytes::from("EX")),
            Frame::Bulk(Bytes::from("60")),
        ]);
        let cmd = Command::from_frame(set_frame).unwrap();
        assert_eq!(
            cmd,
            Command::Set {
                key: "mykey".into(),
                value: Bytes::from("myval"),
                ttl: Some(Duration::from_secs(60)),
            }
        );
        assert!(cmd.is_mutating());
    }

    #[test]
    fn test_execute_set_get() {
        let db = Arc::new(Storage::new());
        let set_cmd = Command::Set {
            key: "user:1".into(),
            value: Bytes::from("Alice"),
            ttl: None,
        };
        let res = set_cmd.execute(&db);
        assert_eq!(res, Frame::Simple("OK".into()));

        let get_cmd = Command::Get("user:1".into());
        let res = get_cmd.execute(&db);
        assert_eq!(res, Frame::Bulk(Bytes::from("Alice")));
    }
}
