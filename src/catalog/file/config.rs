use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

#[derive(Debug)]
pub struct FileCatalogConfig {
    path: PathBuf,
    format: FileFormat,
}

impl FileCatalogConfig {
    pub fn new<P: AsRef<Path>>(path: P) -> Self {
        let path = PathBuf::from(path.as_ref());
        Self {
            path,
            format: FileFormat::Yaml,
        }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn format(&self) -> FileFormat {
        self.format
    }
}

/// The file format where the share configuration is stored.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum FileFormat {
    /// Json file format
    Json,
    /// Yaml file format
    Yaml,
    /// Toml file format
    Toml,
}
