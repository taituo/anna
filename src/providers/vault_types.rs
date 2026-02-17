use std::path::PathBuf;

#[derive(Debug, Clone)]
pub(super) struct VaultConfig {
    pub(super) backend: VaultBackend,
    pub(super) kv_file: PathBuf,
    pub(super) allow_prefixes: Option<Vec<String>>,
    pub(super) read_only: bool,
    pub(super) http: Option<VaultHttpConfig>,
}

#[derive(Debug, Clone, Copy)]
pub(super) enum VaultBackend {
    File,
    Http,
}

#[derive(Debug, Clone)]
pub(super) struct VaultHttpConfig {
    pub(super) addr: String,
    pub(super) auth: VaultAuthConfig,
    pub(super) namespace: Option<String>,
    pub(super) mount: String,
    pub(super) kv_version: u8,
}

#[derive(Debug, Clone)]
pub(super) enum VaultAuthConfig {
    Token(String),
    AppRole {
        role_id: String,
        secret_id: String,
        auth_path: String,
    },
}

#[derive(Debug, Clone, Copy)]
pub(super) enum RenderMode {
    Text,
    Json,
}

#[derive(Debug, Clone)]
pub(super) enum VaultCommand {
    Get { key: String },
    Put { key: String, value: String },
    Delete { key: String },
    List { prefix: Option<String> },
}

impl VaultCommand {
    pub(super) fn is_mutating(&self) -> bool {
        matches!(self, Self::Put { .. } | Self::Delete { .. })
    }
}

#[derive(Debug, Clone)]
pub(super) enum VaultOpResult {
    Get {
        key: String,
        value: String,
    },
    Put {
        key: String,
    },
    Delete {
        key: String,
        deleted: bool,
    },
    List {
        prefix: Option<String>,
        keys: Vec<String>,
    },
}
