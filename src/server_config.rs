use std::{env, fmt};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ServerRole {
    Master,
    Slave,
}

impl fmt::Display for ServerRole {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ServerRole::Master => write!(f, "master"),
            ServerRole::Slave => write!(f, "slave"),
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
#[derive(Debug, Clone)]
pub(crate) struct ServerConfig {
    pub(crate) version: String,
    pub(crate) mode: ServerMode,
    pub(crate) os: String,
    pub(crate) arch_bits: String,
    pub(crate) role: ServerRole,
    pub(crate) replication_id: String,
    pub(crate) replication_offset: usize,
    pub(crate) master_address: Option<String>,
}

impl ServerConfig {
    pub(crate) fn new(
        version: String,
        mode: ServerMode,
        replication_id: String,
        replica_of: Option<String>,
    ) -> Self {
        let arch_bits = if env::consts::ARCH.contains("64") {
            String::from("64")
        } else {
            String::from("32")
        };

        let role;
        let master_address;
        if let Some(replica_of) = replica_of {
            role = ServerRole::Slave;
            let mut address_parts = replica_of.splitn(2, ' ');
            let host = address_parts
                .next()
                .expect("replica_of should have master host");
            let port: u16 = address_parts
                .next()
                .expect("replica_of should have master port")
                .parse()
                .expect("replica_of port should be a integer");

            master_address = Some(format!("{}:{}", host, port));
        } else {
            role = ServerRole::Master;
            master_address = None;
        };

        Self {
            version,
            mode,
            os: env::consts::OS.to_string(),
            arch_bits,
            role,
            replication_id,
            replication_offset: 0,
            master_address,
        }
    }
}
