use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, SystemTime},
};

use tokio::sync::Mutex;

#[derive(Hash, Debug, Clone)]
pub(crate) struct Entry {
    pub(crate) value: String,
    ttl: Option<u128>,
}

impl Entry {
    pub(crate) fn new(value: String, expiration: Option<u128>) -> Self {
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .expect("SystemTime before UNIX EPOCH!")
            .as_millis();

        Self {
            value,
            ttl: expiration.map(|expiration| now + expiration),
        }
    }

    pub(crate) fn has_ttl(&self) -> bool {
        self.ttl.is_some()
    }

    pub(crate) fn is_expired(&self) -> bool {
        if let Some(ttl) = self.ttl {
            let now = SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .expect("SystemTime before UNIX EPOCH!")
                .as_millis();

            now > ttl
        } else {
            false
        }
    }
}

pub(crate) type Db = Arc<Mutex<HashMap<String, Entry>>>;

pub(crate) fn new_db() -> Db {
    Arc::new(Mutex::new(HashMap::<String, Entry>::new()))
}

pub(crate) async fn remove_expired_keys(db: Db) {
    // TODO: improve how keys are expired.
    // https://redis.io/docs/latest/commands/expire/#how-redis-expires-keys
    // https://github.com/valkey-io/valkey/blob/unstable/src/expire.c

    loop {
        tokio::time::sleep(Duration::from_secs(1)).await;
        let mut db = db.lock().await;

        let entries_db = db.clone();
        let keys_to_delete = entries_db.iter().filter(|(_, value)| value.has_ttl());

        for (key, value) in keys_to_delete {
            if value.is_expired() {
                db.remove(key);
                println!("Entry {} deleted", key);
            }
        }
    }
}
