use std::{env, fmt};

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
#[derive(Debug, Clone)]
pub(crate) enum ServerConfig {
    Master {
        version: String,
        mode: ServerMode,
        os: String,
        arch_bits: String,
        replication_id: String,
        replication_offset: usize,
    },
    Slave {
        version: String,
        mode: ServerMode,
        os: String,
        arch_bits: String,
        master_address: String,
    },
}

impl fmt::Display for ServerConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ServerConfig::Master { .. } => write!(f, "master"),
            ServerConfig::Slave { .. } => write!(f, "slave"),
        }
    }
}

impl ServerConfig {
    pub(crate) fn new(version: String, replica_of: Option<String>) -> Self {
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

            Self::Slave {
                version,
                mode,
                os,
                arch_bits,
                master_address: format!("{}:{}", host, port),
            }
        } else {
            Self::Master {
                version,
                mode,
                os,
                arch_bits,
                replication_id: String::from("8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"),
                replication_offset: 0,
            }
        }
    }
}
