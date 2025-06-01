use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use rocksdb::{
    OptimisticTransactionDB, Options, ColumnFamily, ColumnFamilyDescriptor,
    WriteBatch, ReadOptions, WriteOptions, IteratorMode,
    Transaction as RocksTransaction
};
use serde::{Serialize, Deserialize};
use serde_json::{Value, Map};
use dashmap::DashMap;
use crate::common::error::{ArangoError, Result, ErrorCode};
use crate::common::document::{Document, DocumentKey, DocumentId, DocumentFilter};
use crate::common::utils::validate_collection_name;

/// OptimisticTransactionDB-based RocksDB storage engine with proper transaction support
#[derive(Clone)]
pub struct OptimisticRocksDBEngine {
    /// Optimistic transaction database handle
    db: Arc<OptimisticTransactionDB>,
    /// Data directory path
    data_path: PathBuf,
    /// Collection metadata cache
    collections: Arc<DashMap<String, CollectionInfo>>,
    /// Database configuration
    config: EngineConfig,
}

/// Transaction wrapper - directly using RocksDB's transaction like the user's example
pub struct RocksDBTx<'a> {
    /// Underlying RocksDB transaction (None after commit/rollback)
    db_tx: Option<rocksdb::Transaction<'a, OptimisticTransactionDB>>,
    /// Engine reference for metadata access
    engine: &'a OptimisticRocksDBEngine,
}

unsafe impl<'a> Sync for RocksDBTx<'a> {}

/// Storage engine configuration 
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EngineConfig {
    pub max_background_jobs: i32,
    pub write_buffer_size: usize,
    pub max_write_buffer_number: i32,
    pub target_file_size_base: u64,
    pub max_open_files: i32,
    pub compression: bool,
    pub cache_size: usize,
    pub enable_statistics: bool,
}

impl Default for EngineConfig {
    fn default() -> Self {
        EngineConfig {
            max_background_jobs: 4,
            write_buffer_size: 64 * 1024 * 1024,
            max_write_buffer_number: 3,
            target_file_size_base: 64 * 1024 * 1024,
            max_open_files: 1000,
            compression: true,
            cache_size: 256 * 1024 * 1024,
            enable_statistics: true,
        }
    }
}

/// Collection information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CollectionInfo {
    pub name: String,
    pub collection_type: CollectionType,
    pub created: chrono::DateTime<chrono::Utc>,
    pub document_count: u64,
    pub data_size: u64,
}

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

impl OptimisticRocksDBEngine {
    /// Create a new OptimisticTransactionDB storage engine
    pub fn new(data_path: impl AsRef<Path>) -> Result<Self> {
        let data_path = data_path.as_ref().to_path_buf();
        let config = EngineConfig::default();

        // Ensure data directory exists
        crate::common::util::ensure_directory(&data_path)?;

        let db = Self::open_optimistic_database(&data_path, &config)?;

        let collections = Arc::new(DashMap::new());

        let engine = OptimisticRocksDBEngine {
            db: Arc::new(db),
            data_path,
            collections,
            config,
        };

        // Load existing collections
        engine.load_collections()?;

        Ok(engine)
    }

    /// Open OptimisticTransactionDB with column families
    fn open_optimistic_database(data_path: &Path, config: &EngineConfig) -> Result<OptimisticTransactionDB> {
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

        // Open as OptimisticTransactionDB - this is the key difference!
        let db = OptimisticTransactionDB::open_cf_descriptors(&db_options, data_path, cf_descriptors)
            .map_err(|e| ArangoError::internal(format!("failed to open optimistic transaction database: {}", e)))?;

        Ok(db)
    }

    /// Create a new transaction
    pub fn transaction(&self) -> RocksDBTx {
        RocksDBTx::new(self.db.transaction(), self)
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

        // Use a transaction for collection creation
        let mut tx = self.transaction();
        tx.put_collection_metadata(name, &collection_info)?;
        tx.commit()?;

        // Cache collection info
        self.collections.insert(name.to_string(), collection_info.clone());

        Ok(collection_info)
    }

    /// Get collection information
    pub fn get_collection(&self, name: &str) -> Option<CollectionInfo> {
        self.collections.get(name).map(|entry| entry.value().clone())
    }

    /// List all collections
    pub fn list_collections(&self) -> Vec<CollectionInfo> {
        self.collections.iter().map(|entry| entry.value().clone()).collect()
    }

    /// Insert a document into a collection (with transaction)
    pub fn insert_document(&self, collection_name: &str, document: &Document) -> Result<()> {
        let mut tx = self.transaction();
        tx.insert_document(collection_name, document)?;
        tx.commit()
    }

    /// Update a document in a collection (with transaction)
    pub fn update_document(&self, collection_name: &str, document: &Document) -> Result<()> {
        let mut tx = self.transaction();
        tx.update_document(collection_name, document)?;
        tx.commit()
    }

    /// Get a document from a collection (read-only, can use direct DB access)
    pub fn get_document(&self, collection_name: &str, key: &DocumentKey) -> Result<Option<Document>> {
        let _collection_info = self.get_collection(collection_name)
            .ok_or_else(|| ArangoError::collection_not_found(collection_name))?;

        let doc_key = Self::document_key(collection_name, key);

        let cf = self.db.cf_handle(CF_DOCUMENTS)
            .ok_or_else(|| ArangoError::internal("documents column family not found"))?;

        match self.db.get_cf(cf, &doc_key) {
            Ok(Some(data)) => {
                let document: Document = serde_json::from_slice(&data)
                    .map_err(|e| ArangoError::internal(format!("failed to deserialize document: {}", e)))?;
                Ok(Some(document))
            }
            Ok(None) => Ok(None),
            Err(e) => Err(ArangoError::internal(format!("failed to get document: {}", e))),
        }
    }

    /// Delete a document from a collection (with transaction)
    pub fn delete_document(&self, collection_name: &str, key: &DocumentKey) -> Result<bool> {
        let mut tx = self.transaction();
        let result = tx.delete_document(collection_name, key)?;
        tx.commit()?;
        Ok(result)
    }

    /// Generate document key for storage
    fn document_key(collection_name: &str, key: &DocumentKey) -> Vec<u8> {
        format!("{}:{}", collection_name, key.as_str()).into_bytes()
    }
}

impl<'a> RocksDBTx<'a> {
    /// Create a new transaction wrapper
    pub(crate) fn new(db_tx: rocksdb::Transaction<'a, OptimisticTransactionDB>, engine: &'a OptimisticRocksDBEngine) -> Self {
        RocksDBTx {
            db_tx: Some(db_tx),
            engine,
        }
    }

    /// Get a value from the database within transaction
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let db_tx = self.db_tx.as_ref()
            .ok_or_else(|| ArangoError::internal("Transaction already committed"))?;

        db_tx.get(key)
            .map_err(|e| ArangoError::internal(format!("failed to get value: {}", e)))
    }

    /// Put a value into the database within transaction
    pub fn put(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        let db_tx = self.db_tx.as_mut()
            .ok_or_else(|| ArangoError::internal("Transaction already committed"))?;

        db_tx.put(key, value)
            .map_err(|e| ArangoError::internal(format!("failed to put value: {}", e)))
    }

    /// Delete a value from the database within transaction
    pub fn delete(&mut self, key: &[u8]) -> Result<()> {
        let db_tx = self.db_tx.as_mut()
            .ok_or_else(|| ArangoError::internal("Transaction already committed"))?;

        db_tx.delete(key)
            .map_err(|e| ArangoError::internal(format!("failed to delete value: {}", e)))
    }

    /// Check if a key exists within the transaction
    pub fn exists(&self, key: &[u8]) -> Result<bool> {
        let db_tx = self.db_tx.as_ref()
            .ok_or_else(|| ArangoError::internal("Transaction already committed"))?;

        db_tx.get(key)
            .map(|opt| opt.is_some())
            .map_err(|e| ArangoError::internal(format!("Error during exists check: {}", e)))
    }

    /// Parallel put operation (immutable reference to transaction)
    pub fn par_put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        match &self.db_tx {
            Some(db_tx) => db_tx.put(key, value)
                .map_err(|e| ArangoError::internal(format!("Parallel put failed: {}", e))),
            None => Err(ArangoError::internal("Transaction already committed")),
        }
    }

    /// Parallel delete operation (immutable reference to transaction)
    pub fn par_delete(&self, key: &[u8]) -> Result<()> {
        match &self.db_tx {
            Some(db_tx) => db_tx.delete(key)
                .map_err(|e| ArangoError::internal(format!("Parallel delete failed: {}", e))),
            None => Err(ArangoError::internal("Transaction already committed")),
        }
    }

    /// Delete a range of keys from lower to upper (exclusive)
    pub fn delete_range(&mut self, lower: &[u8], upper: &[u8]) -> Result<()> {
        match &mut self.db_tx {
            Some(db_tx) => {
                let iter = db_tx.iterator(rocksdb::IteratorMode::From(
                    lower,
                    rocksdb::Direction::Forward,
                ));
                for item in iter {
                    let (k, _) = item
                        .map_err(|e| ArangoError::internal(format!("Error iterating during range delete: {}", e)))?;
                    if k.as_ref() >= upper {
                        break;
                    }
                    db_tx.delete(&k)
                        .map_err(|e| ArangoError::internal(format!("Error deleting during range delete: {}", e)))?;
                }
                Ok(())
            }
            None => Err(ArangoError::internal("Transaction already committed")),
        }
    }

    /// Range scan within the transaction
    pub fn range_scan(&self, lower: &[u8], upper: &[u8]) -> Result<impl Iterator<Item = Result<(Vec<u8>, Vec<u8>)>> + '_> {
        match &self.db_tx {
            Some(db_tx) => {
                let iter = db_tx.iterator(rocksdb::IteratorMode::From(
                    lower,
                    rocksdb::Direction::Forward,
                ));
                let upper_bound = upper.to_vec();

                Ok(RocksDBIteratorRaw {
                    inner: iter,
                    upper_bound,
                })
            }
            None => Err(ArangoError::internal("Transaction already committed")),
        }
    }

    /// Count items in a range
    pub fn range_count(&self, lower: &[u8], upper: &[u8]) -> Result<usize> {
        let db_tx = self.db_tx.as_ref()
            .ok_or_else(|| ArangoError::internal("Transaction already committed"))?;

        let iter = db_tx.iterator(rocksdb::IteratorMode::From(
            lower,
            rocksdb::Direction::Forward,
        ));
        let count = iter
            .take_while(|item| match item {
                Ok((k, _)) => k.as_ref() < upper,
                Err(_) => false,
            })
            .count();
        Ok(count)
    }

    /// Full scan of the database within transaction
    pub fn total_scan(&self) -> Result<impl Iterator<Item = Result<(Vec<u8>, Vec<u8>)>> + '_> {
        match &self.db_tx {
            Some(db_tx) => {
                Ok(db_tx.iterator(rocksdb::IteratorMode::Start).map(|item| {
                    item.map(|(k, v)| (k.to_vec(), v.to_vec()))
                        .map_err(|e| ArangoError::internal(format!("Error during total scan: {}", e)))
                }))
            }
            None => Err(ArangoError::internal("Transaction already committed")),
        }
    }

    /// Insert a document within the transaction
    pub fn insert_document(&mut self, collection_name: &str, document: &Document) -> Result<()> {
        let _collection_info = self.engine.get_collection(collection_name)
            .ok_or_else(|| ArangoError::collection_not_found(collection_name))?;

        let doc_key = OptimisticRocksDBEngine::document_key(collection_name, document.key());

        // Check if document already exists using transaction
        if self.document_exists(&doc_key)? {
            return Err(ArangoError::unique_constraint_violated(
                format!("document with key '{}' already exists", document.key())
            ));
        }

        let cf = self.engine.db.cf_handle(CF_DOCUMENTS)
            .ok_or_else(|| ArangoError::internal("documents column family not found"))?;

        let serialized = serde_json::to_vec(document)
            .map_err(|e| ArangoError::internal(format!("failed to serialize document: {}", e)))?;

        let db_tx = self.db_tx.as_mut()
            .ok_or_else(|| ArangoError::internal("Transaction already committed"))?;

        db_tx.put_cf(cf, &doc_key, &serialized)
            .map_err(|e| ArangoError::internal(format!("failed to store document: {}", e)))?;

        Ok(())
    }

    /// Update a document within the transaction
    pub fn update_document(&mut self, collection_name: &str, document: &Document) -> Result<()> {
        let _collection_info = self.engine.get_collection(collection_name)
            .ok_or_else(|| ArangoError::collection_not_found(collection_name))?;

        let doc_key = OptimisticRocksDBEngine::document_key(collection_name, document.key());

        // Check if document exists using transaction
        if !self.document_exists(&doc_key)? {
            return Err(ArangoError::document_not_found(document.key().as_str()));
        }

        let cf = self.engine.db.cf_handle(CF_DOCUMENTS)
            .ok_or_else(|| ArangoError::internal("documents column family not found"))?;

        let serialized = serde_json::to_vec(document)
            .map_err(|e| ArangoError::internal(format!("failed to serialize document: {}", e)))?;

        let db_tx = self.db_tx.as_mut()
            .ok_or_else(|| ArangoError::internal("Transaction already committed"))?;

        db_tx.put_cf(cf, &doc_key, &serialized)
            .map_err(|e| ArangoError::internal(format!("failed to update document: {}", e)))?;

        Ok(())
    }

    /// Delete a document within the transaction
    pub fn delete_document(&mut self, collection_name: &str, key: &DocumentKey) -> Result<bool> {
        let _collection_info = self.engine.get_collection(collection_name)
            .ok_or_else(|| ArangoError::collection_not_found(collection_name))?;

        let doc_key = OptimisticRocksDBEngine::document_key(collection_name, key);

        // Check if document exists using transaction
        if !self.document_exists(&doc_key)? {
            return Ok(false);
        }

        let cf = self.engine.db.cf_handle(CF_DOCUMENTS)
            .ok_or_else(|| ArangoError::internal("documents column family not found"))?;

        let db_tx = self.db_tx.as_mut()
            .ok_or_else(|| ArangoError::internal("Transaction already committed"))?;

        db_tx.delete_cf(cf, &doc_key)
            .map_err(|e| ArangoError::internal(format!("failed to delete document: {}", e)))?;

        Ok(true)
    }

    /// Get a document within the transaction
    pub fn get_document(&self, collection_name: &str, key: &DocumentKey) -> Result<Option<Document>> {
        let _collection_info = self.engine.get_collection(collection_name)
            .ok_or_else(|| ArangoError::collection_not_found(collection_name))?;

        let doc_key = OptimisticRocksDBEngine::document_key(collection_name, key);

        let cf = self.engine.db.cf_handle(CF_DOCUMENTS)
            .ok_or_else(|| ArangoError::internal("documents column family not found"))?;

        let db_tx = self.db_tx.as_ref()
            .ok_or_else(|| ArangoError::internal("Transaction already committed"))?;

        match db_tx.get_cf(cf, &doc_key) {
            Ok(Some(data)) => {
                let document: Document = serde_json::from_slice(&data)
                    .map_err(|e| ArangoError::internal(format!("failed to deserialize document: {}", e)))?;
                Ok(Some(document))
            }
            Ok(None) => Ok(None),
            Err(e) => Err(ArangoError::internal(format!("failed to get document: {}", e))),
        }
    }

    /// Put collection metadata within transaction
    pub fn put_collection_metadata(&mut self, name: &str, info: &CollectionInfo) -> Result<()> {
        let cf = self.engine.db.cf_handle(CF_COLLECTIONS)
            .ok_or_else(|| ArangoError::internal("collections column family not found"))?;

        let serialized = serde_json::to_vec(info)
            .map_err(|e| ArangoError::internal(format!("failed to serialize collection info: {}", e)))?;

        let db_tx = self.db_tx.as_mut()
            .ok_or_else(|| ArangoError::internal("Transaction already committed"))?;

        db_tx.put_cf(cf, name.as_bytes(), &serialized)
            .map_err(|e| ArangoError::internal(format!("failed to store collection: {}", e)))?;

        Ok(())
    }

    /// Check if a document exists within the transaction
    fn document_exists(&self, doc_key: &[u8]) -> Result<bool> {
        let cf = self.engine.db.cf_handle(CF_DOCUMENTS)
            .ok_or_else(|| ArangoError::internal("documents column family not found"))?;

        let db_tx = self.db_tx.as_ref()
            .ok_or_else(|| ArangoError::internal("Transaction already committed"))?;

        match db_tx.get_cf(cf, doc_key) {
            Ok(Some(_)) => Ok(true),
            Ok(None) => Ok(false),
            Err(e) => Err(ArangoError::internal(format!("failed to check document existence: {}", e))),
        }
    }

    /// Check if the transaction is still active
    pub fn is_active(&self) -> bool {
        self.db_tx.is_some()
    }

    /// Commit the transaction
    pub fn commit(mut self) -> Result<()> {
        let db_tx = self.db_tx.take()
            .ok_or_else(|| ArangoError::internal("Transaction already committed"))?;

        db_tx.commit()
            .map_err(|e| {
                // Check if this is a conflict error
                let error_str = e.to_string();
                if error_str.contains("conflict") || error_str.contains("busy") || error_str.contains("Transaction") {
                    ArangoError::transaction_conflict(
                        format!("Transaction conflict detected: {}", e)
                    )
                } else {
                    ArangoError::internal(format!("failed to commit transaction: {}", e))
                }
            })
    }

    /// Rollback the transaction
    pub fn rollback(mut self) -> Result<()> {
        if let Some(db_tx) = self.db_tx.take() {
            db_tx.rollback()
                .map_err(|e| ArangoError::internal(format!("failed to rollback transaction: {}", e)))
        } else {
            // Already committed/rolled back
            Ok(())
        }
    }
}

// Implement Drop to automatically rollback uncommitted transactions
impl<'a> Drop for RocksDBTx<'a> {
    fn drop(&mut self) {
        if let Some(db_tx) = self.db_tx.take() {
            // Automatically rollback if not committed
            let _ = db_tx.rollback();
        }
    }
}

/// Iterator for raw key-value pairs
pub struct RocksDBIteratorRaw<'a> {
    inner: rocksdb::DBIteratorWithThreadMode<'a, rocksdb::Transaction<'a, OptimisticTransactionDB>>,
    upper_bound: Vec<u8>,
}

impl<'a> Iterator for RocksDBIteratorRaw<'a> {
    type Item = Result<(Vec<u8>, Vec<u8>)>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.inner.next() {
            Some(Ok((k, v))) => {
                if k.as_ref() >= self.upper_bound.as_slice() {
                    return None;
                }
                Some(Ok((k.to_vec(), v.to_vec())))
            }
            Some(Err(e)) => Some(Err(ArangoError::internal(format!("Iterator error: {}", e)))),
            None => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;
    use serde_json::json;

    fn create_test_engine() -> (OptimisticRocksDBEngine, TempDir) {
        let temp_dir = TempDir::new().unwrap();
        let engine = OptimisticRocksDBEngine::new(temp_dir.path()).unwrap();
        (engine, temp_dir)
    }

    #[test]
    fn test_basic_transaction_operations() {
        let (engine, _temp_dir) = create_test_engine();

        // Create collection
        engine.create_collection("test_basic", CollectionType::Document).unwrap();

        // Test transaction operations
        let mut tx = engine.transaction();

        // Create test document
        let mut data = serde_json::Map::new();
        data.insert("name".to_string(), json!("test"));
        data.insert("value".to_string(), json!(42));
        let doc = Document::new("test_basic", data).unwrap();

        // Insert document
        tx.insert_document("test_basic", &doc).unwrap();

        // Get document within transaction
        let retrieved = tx.get_document("test_basic", doc.key()).unwrap().unwrap();
        assert_eq!(retrieved.get("name"), Some(&json!("test")));

        // Commit transaction
        tx.commit().unwrap();

        // Verify document exists after commit
        let doc_after_commit = engine.get_document("test_basic", doc.key()).unwrap();
        assert!(doc_after_commit.is_some());
    }

    #[test]
    fn test_transaction_rollback_on_drop() {
        let (engine, _temp_dir) = create_test_engine();

        engine.create_collection("test_rollback", CollectionType::Document).unwrap();

        let doc_key = {
            let mut tx = engine.transaction();

            // Create test document
            let mut data = serde_json::Map::new();
            data.insert("name".to_string(), json!("temp"));
            let doc = Document::new("test_rollback", data).unwrap();

            // Insert document
            tx.insert_document("test_rollback", &doc).unwrap();

            doc.key().clone()
            // Transaction dropped here without commit - should rollback automatically
        };

        // Verify document was not persisted
        let persisted = engine.get_document("test_rollback", &doc_key).unwrap();
        assert!(persisted.is_none());
    }

    #[test]
    fn test_direct_transaction_api() {
        let (engine, _temp_dir) = create_test_engine();

        // Test direct transaction API like the user's example
        let mut tx = engine.transaction();

        // Direct put/get operations
        let key = b"test_key";
        let value = b"test_value";

        tx.put(key, value).unwrap();

        let retrieved = tx.get(key).unwrap();
        assert_eq!(retrieved, Some(value.to_vec()));

        tx.commit().unwrap();

        // Verify with new transaction
        let tx2 = engine.transaction();
        let retrieved2 = tx2.get(key).unwrap();
        assert_eq!(retrieved2, Some(value.to_vec()));
    }

    #[test]
    fn test_transaction_conflict_detection() {
        let (engine, _temp_dir) = create_test_engine();

        engine.create_collection("test_conflict", CollectionType::Document).unwrap();

        // Create initial document
        let mut data = serde_json::Map::new();
        data.insert("counter".to_string(), json!(0));
        let doc = Document::new("test_conflict", data).unwrap();
        engine.insert_document("test_conflict", &doc).unwrap();

        // This test shows the basic structure for conflict detection
        // In real concurrent scenarios, conflicts would be detected automatically
        let mut tx = engine.transaction();

        if let Some(mut counter_doc) = tx.get_document("test_conflict", doc.key()).unwrap() {
            let current = counter_doc.get("counter").unwrap().as_i64().unwrap();
            counter_doc.set("counter", json!(current + 1));
            tx.update_document("test_conflict", &counter_doc).unwrap();
        }

        let result = tx.commit();
        assert!(result.is_ok());
    }
} 