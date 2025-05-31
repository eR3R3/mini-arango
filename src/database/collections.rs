use std::sync::Arc;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use serde_json::{Value, Map};
use crate::common::error::{ArangoError, Result};
use crate::common::document::{Document, DocumentKey, DocumentFilter};
use crate::arangod::storage_engine::{StorageEngine, CollectionType};

// Re-export from storage engine
pub use crate::arangod::storage_engine::CollectionInfo;

/// Collection status
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum CollectionStatus {
    NewBorn,
    Unloaded,
    Loaded,
    BeingUnloaded,
    Deleted,
    Loading,
}

/// Collection wrapper around storage engine
pub struct Collection {
    /// Collection information
    info: CollectionInfo,
    /// Storage engine reference
    storage: Arc<dyn StorageEngine>,
}

impl Collection {
    /// Create a new collection wrapper
    pub fn new(info: CollectionInfo, storage: Arc<dyn StorageEngine>) -> Self {
        Collection { info, storage }
    }

    /// Get collection name
    pub fn name(&self) -> &str {
        &self.info.name
    }

    /// Get collection type
    pub fn collection_type(&self) -> CollectionType {
        self.info.collection_type
    }

    /// Get collection information
    pub fn info(&self) -> &CollectionInfo {
        &self.info
    }

    /// Get collection status
    pub fn status(&self) -> CollectionStatus {
        // For now, assume all collections are loaded
        CollectionStatus::Loaded
    }

    /// Insert a document
    pub fn insert(&self, document: &Document) -> Result<()> {
        self.storage.insert_document(&self.info.name, document)
    }

    /// Update a document
    pub fn update(&self, document: &Document) -> Result<()> {
        self.storage.update_document(&self.info.name, document)
    }

    /// Get a document by key
    pub fn get(&self, key: &DocumentKey) -> Result<Option<Document>> {
        self.storage.get_document(&self.info.name, key)
    }

    /// Delete a document by key
    pub fn delete(&self, key: &DocumentKey) -> Result<bool> {
        self.storage.delete_document(&self.info.name, key)
    }

    /// Find documents matching a filter
    pub fn find(&self, filter: &DocumentFilter, limit: Option<usize>) -> Result<Vec<Document>> {
        self.storage.find_documents(&self.info.name, filter, limit)
    }

    /// Count documents in the collection
    pub fn count(&self) -> Result<u64> {
        self.storage.count_documents(&self.info.name)
    }

    /// Get document count from cached info
    pub fn cached_count(&self) -> u64 {
        self.info.document_count
    }

    /// Get data size from cached info
    pub fn data_size(&self) -> u64 {
        self.info.data_size
    }

    /// Check if collection is empty
    pub fn is_empty(&self) -> Result<bool> {
        Ok(self.count()? == 0)
    }
} 