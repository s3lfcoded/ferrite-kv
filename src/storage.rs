use bytes::Bytes;
use std::collections::HashMap;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::RwLock;
use std::time::{Duration, Instant};

const NUM_SHARDS: usize = 64;

/// Represents a single stored value with an optional expiration deadline.
#[derive(Clone, Debug)]
pub struct Entry {
    pub value: Bytes,
    pub expires_at: Option<Instant>,
}

impl Entry {
    pub fn new(value: Bytes, ttl: Option<Duration>) -> Self {
        Self {
            value,
            expires_at: ttl.map(|d| Instant::now() + d),
        }
    }

    pub fn is_expired(&self) -> bool {
        if let Some(deadline) = self.expires_at {
            Instant::now() >= deadline
        } else {
            false
        }
    }
}

/// Sharded in-memory key-value database.
/// Locks are distributed across 64 independent shards to eliminate lock contention.
pub struct Storage {
    shards: Vec<RwLock<HashMap<String, Entry>>>,
}

impl Default for Storage {
    fn default() -> Self {
        Self::new()
    }
}

impl Storage {
    pub fn new() -> Self {
        let mut shards = Vec::with_capacity(NUM_SHARDS);
        for _ in 0..NUM_SHARDS {
            shards.push(RwLock::new(HashMap::new()));
        }
        Self { shards }
    }

    #[inline]
    fn shard_index(&self, key: &str) -> usize {
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        (hasher.finish() as usize) % NUM_SHARDS
    }

    /// Sets a key to hold a string value with an optional TTL.
    pub fn set(&self, key: String, value: Bytes, ttl: Option<Duration>) {
        let idx = self.shard_index(&key);
        let mut shard = self.shards[idx].write().unwrap();
        shard.insert(key, Entry::new(value, ttl));
    }

    /// Gets the value of key. If key does not exist or has expired, returns None.
    pub fn get(&self, key: &str) -> Option<Bytes> {
        let idx = self.shard_index(key);

        // First attempt with a read lock
        {
            let shard = self.shards[idx].read().unwrap();
            {
                let entry = shard.get(key)?;
                if !entry.is_expired() {
                    return Some(entry.value.clone());
                }
            }
        }

        // If expired, acquire write lock to lazily delete
        let mut shard = self.shards[idx].write().unwrap();
        if let Some(entry) = shard.get(key)
            && entry.is_expired()
        {
            shard.remove(key);
        }
        None
    }

    /// Removes specified keys. Returns the number of keys that were removed.
    pub fn del(&self, keys: &[String]) -> usize {
        let mut count = 0;
        for key in keys {
            let idx = self.shard_index(key);
            let mut shard = self.shards[idx].write().unwrap();
            if let Some(entry) = shard.remove(key)
                && !entry.is_expired()
            {
                count += 1;
            }
        }
        count
    }

    /// Returns the number of specified keys that exist (and are not expired).
    pub fn exists(&self, keys: &[String]) -> usize {
        let mut count = 0;
        for key in keys {
            if self.get(key).is_some() {
                count += 1;
            }
        }
        count
    }

    /// Sets a timeout on key in seconds. Returns true if timeout was set, false if key does not exist.
    pub fn expire(&self, key: &str, ttl: Duration) -> bool {
        let idx = self.shard_index(key);
        let mut shard = self.shards[idx].write().unwrap();
        if let Some(entry) = shard.get_mut(key) {
            if entry.is_expired() {
                shard.remove(key);
                false
            } else {
                entry.expires_at = Some(Instant::now() + ttl);
                true
            }
        } else {
            false
        }
    }

    /// Returns remaining time to live in seconds:
    /// - -2 if key does not exist or expired
    /// - -1 if key exists but has no associated expire
    /// - >= 0 remaining seconds
    pub fn ttl(&self, key: &str) -> i64 {
        let idx = self.shard_index(key);
        let shard = self.shards[idx].read().unwrap();
        if let Some(entry) = shard.get(key) {
            if let Some(deadline) = entry.expires_at {
                let now = Instant::now();
                if now >= deadline {
                    -2
                } else {
                    deadline.duration_since(now).as_secs() as i64
                }
            } else {
                -1
            }
        } else {
            -2
        }
    }

    /// Increments the integer value of a key by delta.
    /// If key does not exist, it is set to 0 before performing the operation.
    pub fn incr_by(&self, key: String, delta: i64) -> Result<i64, String> {
        let idx = self.shard_index(&key);
        let mut shard = self.shards[idx].write().unwrap();

        if let Some(entry) = shard.get_mut(&key) {
            if entry.is_expired() {
                let new_val = delta;
                *entry = Entry::new(Bytes::from(new_val.to_string()), None);
                Ok(new_val)
            } else {
                let s = std::str::from_utf8(&entry.value)
                    .map_err(|_| "ERR value is not an integer or out of range")?;
                let current: i64 = s
                    .parse()
                    .map_err(|_| "ERR value is not an integer or out of range")?;
                let new_val = current
                    .checked_add(delta)
                    .ok_or("ERR increment or decrement would overflow")?;
                entry.value = Bytes::from(new_val.to_string());
                Ok(new_val)
            }
        } else {
            shard.insert(key, Entry::new(Bytes::from(delta.to_string()), None));
            Ok(delta)
        }
    }

    /// Returns total number of active (non-expired) keys in the database.
    pub fn dbsize(&self) -> usize {
        let mut total = 0;
        let now = Instant::now();
        for shard in &self.shards {
            let s = shard.read().unwrap();
            for entry in s.values() {
                if let Some(deadline) = entry.expires_at {
                    if now < deadline {
                        total += 1;
                    }
                } else {
                    total += 1;
                }
            }
        }
        total
    }

    /// Purges all expired keys across all shards (Active Expiration).
    pub fn purge_expired(&self) -> usize {
        let mut purged = 0;
        let now = Instant::now();
        for shard in &self.shards {
            let mut s = shard.write().unwrap();
            let before = s.len();
            s.retain(|_, entry| {
                if let Some(deadline) = entry.expires_at {
                    now < deadline
                } else {
                    true
                }
            });
            purged += before - s.len();
        }
        purged
    }

    /// Removes all keys from all shards.
    pub fn flushdb(&self) {
        for shard in &self.shards {
            let mut s = shard.write().unwrap();
            s.clear();
        }
    }

    /// Snapshot of all valid keys and values with their remaining TTL.
    /// Useful for AOF compaction or backup.
    pub fn snapshot(&self) -> Vec<(String, Bytes, Option<Duration>)> {
        let mut result = Vec::new();
        let now = Instant::now();
        for shard in &self.shards {
            let s = shard.read().unwrap();
            for (k, entry) in s.iter() {
                if let Some(deadline) = entry.expires_at {
                    if now < deadline {
                        result.push((k.clone(), entry.value.clone(), Some(deadline - now)));
                    }
                } else {
                    result.push((k.clone(), entry.value.clone(), None));
                }
            }
        }
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_set_and_get() {
        let db = Storage::new();
        db.set("hello".into(), Bytes::from("world"), None);
        assert_eq!(db.get("hello"), Some(Bytes::from("world")));
        assert_eq!(db.get("nonexistent"), None);
    }

    #[test]
    fn test_del_and_exists() {
        let db = Storage::new();
        db.set("k1".into(), Bytes::from("v1"), None);
        db.set("k2".into(), Bytes::from("v2"), None);

        assert_eq!(db.exists(&["k1".into(), "k2".into(), "k3".into()]), 2);
        assert_eq!(db.del(&["k1".into(), "k3".into()]), 1);
        assert_eq!(db.get("k1"), None);
        assert_eq!(db.get("k2"), Some(Bytes::from("v2")));
    }

    #[test]
    fn test_expiration() {
        let db = Storage::new();
        db.set(
            "ephemeral".into(),
            Bytes::from("fast"),
            Some(Duration::from_millis(20)),
        );
        assert_eq!(db.get("ephemeral"), Some(Bytes::from("fast")));

        std::thread::sleep(Duration::from_millis(30));
        // Lazy expiration check
        assert_eq!(db.get("ephemeral"), None);
    }

    #[test]
    fn test_incr_by() {
        let db = Storage::new();
        assert_eq!(db.incr_by("counter".into(), 1).unwrap(), 1);
        assert_eq!(db.incr_by("counter".into(), 5).unwrap(), 6);
        assert_eq!(db.incr_by("counter".into(), -2).unwrap(), 4);
    }
}
