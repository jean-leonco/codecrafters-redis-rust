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
}

impl ServerConfig {
    pub(crate) fn new(
        version: String,
        mode: ServerMode,
        role: ServerRole,
        replication_id: String,
    ) -> Self {
        let arch_bits = if env::consts::ARCH.contains("64") {
            String::from("64")
        } else {
            String::from("32")
        };

        Self {
            version,
            mode,
            os: env::consts::OS.to_string(),
            arch_bits,
            role,
            replication_id,
            replication_offset: 0,
        }
    }
}
