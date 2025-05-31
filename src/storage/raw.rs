use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use rocksdb::{DB, Options, ColumnFamily, ColumnFamilyDescriptor};
use rocksdb::{WriteBatch, ReadOptions, WriteOptions, IteratorMode};
use serde::{Serialize, Deserialize};
use serde_json::{Value, Map};
use dashmap::DashMap;
use crate::common::error::{ArangoError, Result, ErrorCode};
use crate::common::document::{Document, DocumentKey, DocumentId, DocumentFilter};
use crate::common::utils::validate_collection_name;

/// RocksDB storage engine
pub struct RocksDBEngine {
    /// Database handle
    db: Arc<DB>,
    /// Data directory path
    data_path: PathBuf,
    /// Collection metadata cache
    collections: Arc<DashMap<String, CollectionInfo>>,
    /// Database configuration
    config: EngineConfig,
    /// Write options
    write_options: WriteOptions,
    /// Read options
    read_options: ReadOptions,
}

/// Storage engine configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EngineConfig {
    /// Maximum number of background jobs
    pub max_background_jobs: i32,
    /// Write buffer size in bytes
    pub write_buffer_size: usize,
    /// Maximum write buffer number
    pub max_write_buffer_number: i32,
    /// Target file size base
    pub target_file_size_base: u64,
    /// Maximum open files
    pub max_open_files: i32,
    /// Enable compression
    pub compression: bool,
    /// Cache size in bytes
    pub cache_size: usize,
    /// Enable statistics
    pub enable_statistics: bool,
}

impl Default for EngineConfig {
    fn default() -> Self {
        EngineConfig {
            max_background_jobs: 4,
            write_buffer_size: 64 * 1024 * 1024, // 64MB
            max_write_buffer_number: 3,
            target_file_size_base: 64 * 1024 * 1024, // 64MB
            max_open_files: 1000,
            compression: true,
            cache_size: 256 * 1024 * 1024, // 256MB
            enable_statistics: true,
        }
    }
}

/// Collection information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CollectionInfo {
    /// Collection name
    pub name: String,
    /// Collection type (document or edge)
    pub collection_type: CollectionType,
    /// Creation timestamp
    pub created: chrono::DateTime<chrono::Utc>,
    /// Document count (cached)
    pub document_count: u64,
    /// Data size in bytes (cached)
    pub data_size: u64,
}

/// Collection type
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CollectionType {
    Document,
    Edge,
}

impl CollectionType {
    pub fn as_str(&self) -> &'static str {
        match self {
            CollectionType::Document => "document",
            CollectionType::Edge => "edge",
        }
    }
}

/// Column family names
pub const CF_COLLECTIONS: &str = "collections";
pub const CF_DOCUMENTS: &str = "documents";
pub const CF_INDEXES: &str = "indexes";
pub const CF_METADATA: &str = "metadata";

impl RocksDBEngine {
    /// Create a new RocksDB storage engine
    pub fn new(data_path: impl AsRef<Path>) -> Result<Self> {
        let data_path = data_path.as_ref().to_path_buf();
        let config = EngineConfig::default();

        // Ensure data directory exists
        crate::common::utils::ensure_directory(&data_path)?;

        let db = Self::open_database(&data_path, &config)?;

        let collections = Arc::new(DashMap::new());

        // Configure write and read options
        let mut write_options = WriteOptions::default();
        write_options.set_sync(false); // Async writes for performance

        let read_options = ReadOptions::default();

        let engine = RocksDBEngine {
            db: Arc::new(db),
            data_path,
            collections,
            config,
            write_options,
            read_options,
        };

        // Load existing collections
        engine.load_collections()?;

        Ok(engine)
    }

    /// Open RocksDB database with column families
    fn open_database(data_path: &Path, config: &EngineConfig) -> Result<DB> {
        let mut db_options = Options::default();

        // Configure options
        db_options.create_if_missing(true);
        db_options.create_missing_column_families(true);
        db_options.set_max_background_jobs(config.max_background_jobs);
        db_options.set_write_buffer_size(config.write_buffer_size);
        db_options.set_max_write_buffer_number(config.max_write_buffer_number);
        db_options.set_target_file_size_base(config.target_file_size_base);
        db_options.set_max_open_files(config.max_open_files);

        if config.compression {
            db_options.set_compression_type(rocksdb::DBCompressionType::Snappy);
        }

        if config.enable_statistics {
            db_options.enable_statistics();
        }

        // Define column families
        let cf_descriptors = vec![
            ColumnFamilyDescriptor::new(CF_COLLECTIONS, Options::default()),
            ColumnFamilyDescriptor::new(CF_DOCUMENTS, Options::default()),
            ColumnFamilyDescriptor::new(CF_INDEXES, Options::default()),
            ColumnFamilyDescriptor::new(CF_METADATA, Options::default()),
        ];

        let db = DB::open_cf_descriptors(&db_options, data_path, cf_descriptors)
            .map_err(|e| ArangoError::internal(format!("failed to open database: {}", e)))?;

        Ok(db)
    }

    /// Load existing collections from storage
    fn load_collections(&self) -> Result<()> {
        let cf = self.db.cf_handle(CF_COLLECTIONS)
            .ok_or_else(|| ArangoError::internal("collections column family not found"))?;

        let iterator = self.db.iterator_cf(cf, IteratorMode::Start);

        for item in iterator {
            let (key, value) = item.map_err(|e| ArangoError::internal(format!("iterator error: {}", e)))?;

            let collection_name = String::from_utf8(key.to_vec())
                .map_err(|_| ArangoError::internal("invalid collection name encoding"))?;

            let collection_info: CollectionInfo = serde_json::from_slice(&value)
                .map_err(|e| ArangoError::internal(format!("failed to deserialize collection info: {}", e)))?;

            self.collections.insert(collection_name, collection_info);
        }

        Ok(())
    }

    /// Create a new collection
    pub fn create_collection(&self, name: &str, collection_type: CollectionType) -> Result<CollectionInfo> {
        validate_collection_name(name)?;

        if self.collections.contains_key(name) {
            return Err(ArangoError::new(
                ErrorCode::ArangoDuplicateName,
                format!("collection '{}' already exists", name)
            ));
        }

        let collection_info = CollectionInfo {
            name: name.to_string(),
            collection_type,
            created: chrono::Utc::now(),
            document_count: 0,
            data_size: 0,
        };

        // Store collection metadata
        let cf = self.db.cf_handle(CF_COLLECTIONS)
            .ok_or_else(|| ArangoError::internal("collections column family not found"))?;

        let serialized = serde_json::to_vec(&collection_info)
            .map_err(|e| ArangoError::internal(format!("failed to serialize collection info: {}", e)))?;

        self.db.put_cf_opt(cf, name.as_bytes(), &serialized, &self.write_options)
            .map_err(|e| ArangoError::internal(format!("failed to store collection: {}", e)))?;

        // Cache collection info
        self.collections.insert(name.to_string(), collection_info.clone());

        Ok(collection_info)
    }

    /// Drop a collection
    pub fn drop_collection(&self, name: &str) -> Result<()> {
        if !self.collections.contains_key(name) {
            return Err(ArangoError::collection_not_found(name));
        }

        // Delete all documents in the collection
        self.delete_all_documents(name)?;

        // Remove collection metadata
        let cf = self.db.cf_handle(CF_COLLECTIONS)
            .ok_or_else(|| ArangoError::internal("collections column family not found"))?;

        self.db.delete_cf_opt(cf, name.as_bytes(), &self.write_options)
            .map_err(|e| ArangoError::internal(format!("failed to delete collection: {}", e)))?;

        // Remove from cache
        self.collections.remove(name);

        Ok(())
    }

    /// Get collection information
    pub fn get_collection(&self, name: &str) -> Option<CollectionInfo> {
        self.collections.get(name).map(|entry| entry.value().clone())
    }

    /// List all collections
    pub fn list_collections(&self) -> Vec<CollectionInfo> {
        self.collections.iter().map(|entry| entry.value().clone()).collect()
    }

    /// Insert a document into a collection
    pub fn insert_document(&self, collection_name: &str, document: &Document) -> Result<()> {
        let _collection_info = self.get_collection(collection_name)
            .ok_or_else(|| ArangoError::collection_not_found(collection_name))?;

        let doc_key = Self::document_key(collection_name, document.key());

        // Check if document already exists
        if self.document_exists(&doc_key)? {
            return Err(ArangoError::unique_constraint_violated(
                format!("document with key '{}' already exists", document.key())
            ));
        }

        let cf = self.db.cf_handle(CF_DOCUMENTS)
            .ok_or_else(|| ArangoError::internal("documents column family not found"))?;

        let serialized = serde_json::to_vec(document)
            .map_err(|e| ArangoError::internal(format!("failed to serialize document: {}", e)))?;

        self.db.put_cf_opt(cf, &doc_key, &serialized, &self.write_options)
            .map_err(|e| ArangoError::internal(format!("failed to store document: {}", e)))?;

        // Update collection stats
        self.update_collection_stats(collection_name, 1, serialized.len() as i64)?;

        Ok(())
    }

    /// Update a document in a collection
    pub fn update_document(&self, collection_name: &str, document: &Document) -> Result<()> {
        let _collection_info = self.get_collection(collection_name)
            .ok_or_else(|| ArangoError::collection_not_found(collection_name))?;

        let doc_key = Self::document_key(collection_name, document.key());

        // Check if document exists
        if !self.document_exists(&doc_key)? {
            return Err(ArangoError::document_not_found(document.key().as_str()));
        }

        let cf = self.db.cf_handle(CF_DOCUMENTS)
            .ok_or_else(|| ArangoError::internal("documents column family not found"))?;

        let serialized = serde_json::to_vec(document)
            .map_err(|e| ArangoError::internal(format!("failed to serialize document: {}", e)))?;

        self.db.put_cf_opt(cf, &doc_key, &serialized, &self.write_options)
            .map_err(|e| ArangoError::internal(format!("failed to update document: {}", e)))?;

        Ok(())
    }

    /// Get a document from a collection
    pub fn get_document(&self, collection_name: &str, key: &DocumentKey) -> Result<Option<Document>> {
        let _collection_info = self.get_collection(collection_name)
            .ok_or_else(|| ArangoError::collection_not_found(collection_name))?;

        let doc_key = Self::document_key(collection_name, key);

        let cf = self.db.cf_handle(CF_DOCUMENTS)
            .ok_or_else(|| ArangoError::internal("documents column family not found"))?;

        match self.db.get_cf_opt(cf, &doc_key, &self.read_options) {
            Ok(Some(data)) => {
                let document: Document = serde_json::from_slice(&data)
                    .map_err(|e| ArangoError::internal(format!("failed to deserialize document: {}", e)))?;
                Ok(Some(document))
            }
            Ok(None) => Ok(None),
            Err(e) => Err(ArangoError::internal(format!("failed to get document: {}", e))),
        }
    }

    /// Delete a document from a collection
    pub fn delete_document(&self, collection_name: &str, key: &DocumentKey) -> Result<bool> {
        let _collection_info = self.get_collection(collection_name)
            .ok_or_else(|| ArangoError::collection_not_found(collection_name))?;

        let doc_key = Self::document_key(collection_name, key);

        // Check if document exists and get its size for stats
        let document_size = if let Some(document) = self.get_document(collection_name, key)? {
            document.size()
        } else {
            return Ok(false); // Document doesn't exist
        };

        let cf = self.db.cf_handle(CF_DOCUMENTS)
            .ok_or_else(|| ArangoError::internal("documents column family not found"))?;

        self.db.delete_cf_opt(cf, &doc_key, &self.write_options)
            .map_err(|e| ArangoError::internal(format!("failed to delete document: {}", e)))?;

        // Update collection stats
        self.update_collection_stats(collection_name, -1, -(document_size as i64))?;

        Ok(true)
    }

    /// Find documents in a collection matching a filter
    pub fn find_documents(&self, collection_name: &str, filter: &DocumentFilter, limit: Option<usize>) -> Result<Vec<Document>> {
        let _collection_info = self.get_collection(collection_name)
            .ok_or_else(|| ArangoError::collection_not_found(collection_name))?;

        let cf = self.db.cf_handle(CF_DOCUMENTS)
            .ok_or_else(|| ArangoError::internal("documents column family not found"))?;

        let prefix = format!("{}:", collection_name);
        let iterator = self.db.prefix_iterator_cf(cf, prefix.as_bytes());

        let mut results = Vec::new();
        let mut count = 0;

        for item in iterator {
            if let Some(max_count) = limit {
                if count >= max_count {
                    break;
                }
            }

            let (_, value) = item.map_err(|e| ArangoError::internal(format!("iterator error: {}", e)))?;

            let document: Document = serde_json::from_slice(&value)
                .map_err(|e| ArangoError::internal(format!("failed to deserialize document: {}", e)))?;

            if filter.matches(&document) {
                results.push(document);
                count += 1;
            }
        }

        Ok(results)
    }

    /// Count documents in a collection
    pub fn count_documents(&self, collection_name: &str) -> Result<u64> {
        if let Some(collection_info) = self.get_collection(collection_name) {
            Ok(collection_info.document_count)
        } else {
            Err(ArangoError::collection_not_found(collection_name))
        }
    }

    /// Delete all documents in a collection
    fn delete_all_documents(&self, collection_name: &str) -> Result<()> {
        let cf = self.db.cf_handle(CF_DOCUMENTS)
            .ok_or_else(|| ArangoError::internal("documents column family not found"))?;

        let prefix = format!("{}:", collection_name);
        let iterator = self.db.prefix_iterator_cf(cf, prefix.as_bytes());

        let mut batch = WriteBatch::default();

        for item in iterator {
            let (key, _) = item.map_err(|e| ArangoError::internal(format!("iterator error: {}", e)))?;
            batch.delete_cf(cf, &key);
        }

        self.db.write_opt(batch, &self.write_options)
            .map_err(|e| ArangoError::internal(format!("failed to delete documents: {}", e)))?;

        // Reset collection stats
        self.reset_collection_stats(collection_name)?;

        Ok(())
    }

    /// Check if a document exists
    fn document_exists(&self, doc_key: &[u8]) -> Result<bool> {
        let cf = self.db.cf_handle(CF_DOCUMENTS)
            .ok_or_else(|| ArangoError::internal("documents column family not found"))?;

        match self.db.get_cf_opt(cf, doc_key, &self.read_options) {
            Ok(Some(_)) => Ok(true),
            Ok(None) => Ok(false),
            Err(e) => Err(ArangoError::internal(format!("failed to check document existence: {}", e))),
        }
    }

    /// Generate document key for storage
    fn document_key(collection_name: &str, key: &DocumentKey) -> Vec<u8> {
        format!("{}:{}", collection_name, key.as_str()).into_bytes()
    }

    /// Update collection statistics
    fn update_collection_stats(&self, collection_name: &str, doc_count_delta: i64, size_delta: i64) -> Result<()> {
        if let Some(mut collection_entry) = self.collections.get_mut(collection_name) {
            let collection_info = collection_entry.value_mut();

            if doc_count_delta > 0 {
                collection_info.document_count += doc_count_delta as u64;
            } else if doc_count_delta < 0 && collection_info.document_count > 0 {
                collection_info.document_count = collection_info.document_count.saturating_sub((-doc_count_delta) as u64);
            }

            if size_delta > 0 {
                collection_info.data_size += size_delta as u64;
            } else if size_delta < 0 && collection_info.data_size > 0 {
                collection_info.data_size = collection_info.data_size.saturating_sub((-size_delta) as u64);
            }

            // Persist updated collection info
            let cf = self.db.cf_handle(CF_COLLECTIONS)
                .ok_or_else(|| ArangoError::internal("collections column family not found"))?;

            let serialized = serde_json::to_vec(collection_info)
                .map_err(|e| ArangoError::internal(format!("failed to serialize collection info: {}", e)))?;

            self.db.put_cf_opt(cf, collection_name.as_bytes(), &serialized, &self.write_options)
                .map_err(|e| ArangoError::internal(format!("failed to update collection stats: {}", e)))?;
        }

        Ok(())
    }

    /// Reset collection statistics
    fn reset_collection_stats(&self, collection_name: &str) -> Result<()> {
        if let Some(mut collection_entry) = self.collections.get_mut(collection_name) {
            let collection_info = collection_entry.value_mut();
            collection_info.document_count = 0;
            collection_info.data_size = 0;

            // Persist updated collection info
            let cf = self.db.cf_handle(CF_COLLECTIONS)
                .ok_or_else(|| ArangoError::internal("collections column family not found"))?;

            let serialized = serde_json::to_vec(collection_info)
                .map_err(|e| ArangoError::internal(format!("failed to serialize collection info: {}", e)))?;

            self.db.put_cf_opt(cf, collection_name.as_bytes(), &serialized, &self.write_options)
                .map_err(|e| ArangoError::internal(format!("failed to reset collection stats: {}", e)))?;
        }

        Ok(())
    }

    /// Get database statistics
    pub fn get_statistics(&self) -> Result<DatabaseStatistics> {
        let total_collections = self.collections.len();
        let mut total_documents = 0u64;
        let mut total_size = 0u64;

        for collection in self.collections.iter() {
            total_documents += collection.document_count;
            total_size += collection.data_size;
        }

        // Get RocksDB statistics if available
        let rocks_stats = if let Some(stats) = self.db.get_property("rocksdb.stats") {
            Some(stats)
        } else {
            None
        };

        Ok(DatabaseStatistics {
            total_collections,
            total_documents,
            total_size,
            rocks_stats,
        })
    }

    /// Compact database
    pub fn compact(&self) -> Result<()> {
        self.db.compact_range::<&[u8], &[u8]>(None, None);
        Ok(())
    }

    /// Close the database
    pub fn close(&self) -> Result<()> {
        // RocksDB will be closed when the DB is dropped
        Ok(())
    }
}

/// Database statistics
#[derive(Debug, Serialize, Deserialize)]
pub struct DatabaseStatistics {
    pub total_collections: usize,
    pub total_documents: u64,
    pub total_size: u64,
    pub rocks_stats: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;
    use serde_json::json;

    fn create_test_engine() -> (RocksDBEngine, TempDir) {
        let temp_dir = TempDir::new().unwrap();
        let engine = RocksDBEngine::new(temp_dir.path()).unwrap();
        (engine, temp_dir)
    }

    #[test]
    fn test_collection_operations() {
        let (engine, _temp_dir) = create_test_engine();

        // Create collection
        let collection_info = engine.create_collection("test_collection", CollectionType::Document).unwrap();
        assert_eq!(collection_info.name, "test_collection");
        assert_eq!(collection_info.collection_type, CollectionType::Document);

        // Get collection
        let retrieved = engine.get_collection("test_collection").unwrap();
        assert_eq!(retrieved.name, "test_collection");

        // List collections
        let collections = engine.list_collections();
        assert_eq!(collections.len(), 1);

        // Drop collection
        engine.drop_collection("test_collection").unwrap();
        assert!(engine.get_collection("test_collection").is_none());
    }

    #[test]
    fn test_document_operations() {
        let (engine, _temp_dir) = create_test_engine();

        // Create collection
        engine.create_collection("test_docs", CollectionType::Document).unwrap();

        // Create test document
        let mut data = serde_json::Map::new();
        data.insert("name".to_string(), json!("test"));
        data.insert("value".to_string(), json!(42));
        let doc = Document::new("test_docs", data).unwrap();

        // Insert document
        engine.insert_document("test_docs", &doc).unwrap();

        // Get document
        let retrieved = engine.get_document("test_docs", doc.key()).unwrap().unwrap();
        assert_eq!(retrieved.get("name"), Some(&json!("test")));
        assert_eq!(retrieved.get("value"), Some(&json!(42)));

        // Count documents
        let count = engine.count_documents("test_docs").unwrap();
        assert_eq!(count, 1);

        // Delete document
        let deleted = engine.delete_document("test_docs", doc.key()).unwrap();
        assert!(deleted);

        // Verify deletion
        let retrieved = engine.get_document("test_docs", doc.key()).unwrap();
        assert!(retrieved.is_none());
    }
} 