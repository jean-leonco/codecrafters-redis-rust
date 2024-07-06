use std::{collections::HashMap, sync::Arc, time::SystemTime};

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
            ttl: match expiration {
                Some(expiration) => Some(now + expiration),
                None => None,
            },
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
            return false;
        }
    }
}

pub(crate) type Db = Arc<Mutex<HashMap<String, Entry>>>;

pub(crate) fn new_db() -> Db {
    Arc::new(Mutex::new(HashMap::<String, Entry>::new()))
}
