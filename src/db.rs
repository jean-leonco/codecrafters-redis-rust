use std::{
    collections::HashMap,
    env, fmt,
    sync::Arc,
    time::{Duration, SystemTime},
};

use tokio::sync::broadcast;

use crate::{
    commands::{set, Command},
    message::Message,
};

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

#[derive(Debug, Clone)]
pub(crate) enum ServerMode {
    Standalone,
}

impl fmt::Display for ServerMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ServerMode::Standalone => write!(f, "standalone"),
        }
    }
}

#[derive(Debug)]
pub(crate) enum State {
    Master {
        version: String,
        mode: ServerMode,
        os: String,
        arch_bits: String,
        replication_id: String,
        replication_offset: usize,
        entries: tokio::sync::Mutex<HashMap<String, Entry>>,
        tx: tokio::sync::broadcast::Sender<Message>,
    },
    Slave {
        version: String,
        mode: ServerMode,
        os: String,
        arch_bits: String,
        master_address: String,
        entries: tokio::sync::Mutex<HashMap<String, Entry>>,
    },
}

#[derive(Debug, Clone)]
pub(crate) struct Db {
    pub(crate) state: Arc<State>,
}

impl Db {
    pub(crate) fn new(replica_of: Option<String>) -> Self {
        let version = String::from("0.0.0");
        let os = env::consts::OS.to_string();
        let mode = ServerMode::Standalone;
        let arch_bits = if env::consts::ARCH.contains("64") {
            String::from("64")
        } else {
            String::from("32")
        };

        if let Some(replica_of) = replica_of {
            let mut address_parts = replica_of.splitn(2, ' ');
            let host = address_parts
                .next()
                .expect("replica_of should have master host");
            let port: u16 = address_parts
                .next()
                .expect("replica_of should have master port")
                .parse()
                .expect("replica_of port should be a integer");

            Self {
                state: Arc::new(State::Slave {
                    version,
                    mode,
                    os,
                    arch_bits,
                    master_address: format!("{}:{}", host, port),
                    entries: tokio::sync::Mutex::new(HashMap::new()),
                }),
            }
        } else {
            let (tx, mut _rx) = broadcast::channel(1024);

            Self {
                state: Arc::new(State::Master {
                    version,
                    mode,
                    os,
                    arch_bits,
                    replication_id: String::from("8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"),
                    replication_offset: 0,
                    entries: tokio::sync::Mutex::new(HashMap::new()),
                    tx,
                }),
            }
        }
    }

    async fn get_entries(&self) -> tokio::sync::MutexGuard<HashMap<String, Entry>> {
        match &*self.state {
            State::Master { entries, .. } => entries.lock().await,
            State::Slave { entries, .. } => entries.lock().await,
        }
    }

    pub(crate) async fn get_key(&self, key: &String) -> Option<Entry> {
        let mut entries = self.get_entries().await;

        match entries.get(key) {
            Some(value) if value.is_expired() => {
                entries.remove(key);
                None
            }
            Some(value) => Some(value.clone()),
            None => None,
        }
    }

    pub(crate) async fn insert_key(&self, key: String, value: Entry) {
        let mut entries = self.get_entries().await;

        entries.insert(key.clone(), value.clone());

        if let State::Master { .. } = &*self.state {
            self.propagate_command_to_replicas(set::SetCommand::new_command(
                key,
                value.value,
                value.ttl,
            ))
            .await;
        }
    }

    async fn propagate_command_to_replicas(&self, command: impl Command) {
        match &*self.state {
            State::Master { tx, .. } => {
                if tx.receiver_count() > 0 {
                    tx.send(command.to_message()).unwrap();
                }
            }
            _ => unreachable!(),
        }
    }

    pub(crate) async fn remove_expired_keys(&self) {
        // TODO: improve how keys are expired.
        // https://redis.io/docs/latest/commands/expire/#how-redis-expires-keys
        // https://github.com/valkey-io/valkey/blob/unstable/src/expire.c

        loop {
            tokio::time::sleep(Duration::from_secs(1)).await;

            let mut entries = self.get_entries().await;

            let entries_to_check = entries.clone();
            let keys_to_delete = entries_to_check.iter().filter(|(_, value)| value.has_ttl());

            for (key, value) in keys_to_delete {
                if value.is_expired() {
                    entries.remove(key);
                    println!("Entry {} deleted", key);
                }
            }
        }
    }
}
