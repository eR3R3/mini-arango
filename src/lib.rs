// Mini ArangoDB Library
// Core functionality for a Rust implementation of ArangoDB

// Common modules
pub mod common {
    pub mod error;
    pub mod document;
    pub mod value;
    pub mod utils;
}

// ArangoDB daemon modules
pub mod arangod {
    pub mod storage_engine;
    pub mod database;
    pub mod transaction;
    pub mod scheduler;
    pub mod indexes;
    pub mod cache;
    pub mod graph;
    pub mod feature_phases;
}

// Re-export commonly used types for convenience
pub use common::error::{ArangoError, Result};
pub use common::document::{Document, DocumentKey, DocumentId, DocumentFilter};
pub use common::value::ArangoValue;

pub use arangod::storage_engine::{
    OptimisticRocksDBEngine,
    RocksDBEngine,
    StorageEngine,
    CollectionType,
    CollectionInfo,
    DatabaseStatistics
};

// Version information
pub const VERSION: &str = env!("CARGO_PKG_VERSION");
pub const NAME: &str = env!("CARGO_PKG_NAME");

/// Mini ArangoDB library initialization
pub fn init() -> Result<()> {
    // Initialize logging if not already done
    if std::env::var("RUST_LOG").is_err() {
        std::env::set_var("RUST_LOG", "info");
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_library_initialization() {
        assert!(init().is_ok());
    }

    #[test]
    fn test_version_info() {
        assert!(!VERSION.is_empty());
        assert!(!NAME.is_empty());
    }
} 