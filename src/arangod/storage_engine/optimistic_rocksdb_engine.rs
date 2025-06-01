use std::collections::{HashMap, HashSet};
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

/// Collection schema definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CollectionSchema {
    /// Field definitions
    pub fields: HashMap<String, FieldDefinition>,
    /// Required fields that must be present
    pub required_fields: HashSet<String>,
    /// Whether to enforce strict schema (reject extra fields)
    pub strict_mode: bool,
    /// Schema version for evolution
    pub version: u32,
}

impl Default for CollectionSchema {
    fn default() -> Self {
        CollectionSchema {
            fields: HashMap::new(),
            required_fields: HashSet::new(),
            strict_mode: false,
            version: 1,
        }
    }
}

/// Field definition with type and constraints
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FieldDefinition {
    /// Field data type
    pub field_type: FieldType,
    /// Whether field can be null
    pub nullable: bool,
    /// Default value if not provided
    pub default_value: Option<Value>,
    /// Field constraints
    pub constraints: Vec<FieldConstraint>,
    /// Field description
    pub description: Option<String>,
}

/// Supported field types
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum FieldType {
    /// String type with optional max length
    String { max_length: Option<usize> },
    /// Integer number
    Integer,
    /// Floating point number
    Float,
    /// Boolean value
    Boolean,
    /// Array of elements
    Array { element_type: Box<FieldType> },
    /// Nested object
    Object { schema: Option<CollectionSchema> },
    /// Date/timestamp
    Date,
    /// Binary data
    Binary,
    /// Any type (no validation)
    Any,
}

/// Field constraints
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum FieldConstraint {
    /// Minimum value (for numbers)
    MinValue(f64),
    /// Maximum value (for numbers)
    MaxValue(f64),
    /// Minimum length (for strings/arrays)
    MinLength(usize),
    /// Maximum length (for strings/arrays)
    MaxLength(usize),
    /// Regex pattern (for strings)
    Pattern(String),
    /// Enum values
    OneOf(Vec<Value>),
    /// Custom validation expression
    Custom(String),
}

/// Collection information with schema support
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CollectionInfo {
    pub name: String,
    pub collection_type: CollectionType,
    pub created: chrono::DateTime<chrono::Utc>,
    pub document_count: u64,
    pub data_size: u64,
    /// Optional schema definition
    pub schema: Option<CollectionSchema>,
    /// Collection-level options
    pub options: CollectionOptions,
}

/// Collection creation options
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CollectionOptions {
    /// Wait for sync on operations
    pub wait_for_sync: bool,
    /// Enable automatic key generation
    pub key_generator: KeyGenerator,
    /// Maximum document size
    pub max_document_size: Option<usize>,
    /// Enable schema validation
    pub validate_schema: bool,
}

impl Default for CollectionOptions {
    fn default() -> Self {
        CollectionOptions {
            wait_for_sync: false,
            key_generator: KeyGenerator::Traditional,
            max_document_size: Some(16 * 1024 * 1024), // 16MB
            validate_schema: true,
        }
    }
}

/// Key generation strategies
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum KeyGenerator {
    /// Traditional string keys
    Traditional,
    /// Auto-increment integer keys
    AutoIncrement,
    /// UUID v4 keys
    Uuid,
}

/// Transaction wrapper - directly using RocksDB's transaction like the user's example
pub struct RocksDBTx<'a> {
    /// Underlying RocksDB transaction (None after commit/rollback)
    db_tx: Option<rocksdb::Transaction<'a, OptimisticTransactionDB>>,
    /// Engine reference for metadata access
    engine: &'a OptimisticRocksDBEngine,
    /// Track collection statistics changes during transaction
    stats_changes: HashMap<String, (i64, i64)>, // (document_count_change, data_size_change)
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
        crate::common::utils::ensure_directory(&data_path)?;

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
        self.create_collection_with_schema(name, collection_type, None, CollectionOptions::default())
    }

    /// Create a new collection with schema and options
    pub fn create_collection_with_schema(
        &self,
        name: &str,
        collection_type: CollectionType,
        schema: Option<CollectionSchema>,
        options: CollectionOptions
    ) -> Result<CollectionInfo> {
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
            schema,
            options,
        };

        // Use a transaction for collection creation
        let mut tx = self.transaction();
        tx.put_collection_metadata(name, &collection_info)?;
        tx.commit()?;

        // Cache collection info
        self.collections.insert(name.to_string(), collection_info.clone());

        Ok(collection_info)
    }

    /// Update collection schema
    pub fn update_collection_schema(&self, name: &str, schema: CollectionSchema) -> Result<()> {
        let mut collection_info = self.get_collection(name)
            .ok_or_else(|| ArangoError::collection_not_found(name))?;

        collection_info.schema = Some(schema);

        // Update in storage
        let mut tx = self.transaction();
        tx.put_collection_metadata(name, &collection_info)?;
        tx.commit()?;

        // Update cache
        self.collections.insert(name.to_string(), collection_info);

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

    pub fn count_documents(&self, collection_name: &str) -> Result<u64> {
        let cf_handle = self.db.cf_handle(&format!("documents_{}", collection_name))
            .ok_or_else(|| ArangoError::collection_not_found(collection_name))?;

        let mut count = 0u64;
        let iter = self.db.iterator_cf(cf_handle, rocksdb::IteratorMode::Start);

        for item in iter {
            let _ = item.map_err(|e| ArangoError::internal(format!("Iterator error: {}", e)))?;
            count += 1;
        }

        Ok(count)
    }

    /// Get all documents in a collection
    pub fn get_all_documents_in_collection(&self, collection_name: &str) -> Result<Vec<Document>> {
        // Verify collection exists
        let _collection_info = self.get_collection(collection_name)
            .ok_or_else(|| ArangoError::collection_not_found(collection_name))?;

        let mut documents = Vec::new();

        // Get the documents column family
        let cf = self.db.cf_handle(CF_DOCUMENTS)
            .ok_or_else(|| ArangoError::internal("documents column family not found"))?;

        // Create an iterator for the documents column family
        let iter = self.db.iterator_cf(cf, rocksdb::IteratorMode::Start);

        // Prefix pattern: "collection_name:"
        let prefix = format!("{}:", collection_name);

        for item in iter {
            if let Ok((key_bytes, value_bytes)) = item {
                // Convert key bytes to string
                if let Ok(key_str) = String::from_utf8(key_bytes.to_vec()) {
                    // Check if this key belongs to our collection
                    if key_str.starts_with(&prefix) {
                        // Try to deserialize the document
                        if let Ok(document) = serde_json::from_slice::<Document>(&value_bytes) {
                            documents.push(document);
                        }
                    }
                }
            }
        }

        Ok(documents)
    }
}

impl<'a> RocksDBTx<'a> {
    /// Create a new transaction wrapper
    pub(crate) fn new(db_tx: rocksdb::Transaction<'a, OptimisticTransactionDB>, engine: &'a OptimisticRocksDBEngine) -> Self {
        RocksDBTx {
            db_tx: Some(db_tx),
            engine,
            stats_changes: HashMap::new(),
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
        let collection_info = self.engine.get_collection(collection_name)
            .ok_or_else(|| ArangoError::collection_not_found(collection_name))?;

        // Validate schema if present and enabled
        if collection_info.options.validate_schema {
            if let Some(ref schema) = collection_info.schema {
                schema.validate_document(document)?;
            }
        }

        let doc_key = OptimisticRocksDBEngine::document_key(collection_name, document.key());

        // Check if document already exists using transaction
        if self.document_exists(&doc_key)? {
            return Err(ArangoError::unique_constraint_violated(
                format!("document with key '{}' already exists", document.key())
            ));
        }

        let cf = self.engine.db.cf_handle(CF_DOCUMENTS)
            .ok_or_else(|| ArangoError::internal("documents column family not found"))?;

        let data = &document.data;

        let serialized = serde_json::to_vec(document)
            .map_err(|e| ArangoError::internal(format!("failed to serialize document: {}", e)))?;

        let db_tx = self.db_tx.as_mut()
            .ok_or_else(|| ArangoError::internal("Transaction already committed"))?;

        db_tx.put_cf(cf, &doc_key, &serialized)
            .map_err(|e| ArangoError::internal(format!("failed to store document: {}", e)))?;

        // Track statistics changes: +1 document, +size bytes
        let document_size = serialized.len() as i64;
        let (count_change, size_change) = self.stats_changes.entry(collection_name.to_string())
            .or_insert((0, 0));
        *count_change += 1;
        *size_change += document_size;

        Ok(())
    }

    /// Update a document within the transaction
    pub fn update_document(&mut self, collection_name: &str, document: &Document) -> Result<()> {
        let collection_info = self.engine.get_collection(collection_name)
            .ok_or_else(|| ArangoError::collection_not_found(collection_name))?;

        // Validate schema if present and enabled
        if collection_info.options.validate_schema {
            if let Some(ref schema) = collection_info.schema {
                schema.validate_document(document)?;
            }
        }

        let doc_key = OptimisticRocksDBEngine::document_key(collection_name, document.key());

        // Check if document exists and get old size
        let cf = self.engine.db.cf_handle(CF_DOCUMENTS)
            .ok_or_else(|| ArangoError::internal("documents column family not found"))?;

        let db_tx = self.db_tx.as_ref()
            .ok_or_else(|| ArangoError::internal("Transaction already committed"))?;

        let old_size = match db_tx.get_cf(cf, &doc_key) {
            Ok(Some(old_data)) => old_data.len() as i64,
            Ok(None) => return Err(ArangoError::document_not_found(document.key().as_str())),
            Err(e) => return Err(ArangoError::internal(format!("failed to get existing document: {}", e))),
        };

        let serialized = serde_json::to_vec(document)
            .map_err(|e| ArangoError::internal(format!("failed to serialize document: {}", e)))?;

        let db_tx = self.db_tx.as_mut()
            .ok_or_else(|| ArangoError::internal("Transaction already committed"))?;

        db_tx.put_cf(cf, &doc_key, &serialized)
            .map_err(|e| ArangoError::internal(format!("failed to update document: {}", e)))?;

        // Track statistics changes: 0 document count change, +/- size difference
        let new_size = serialized.len() as i64;
        let size_change = new_size - old_size;

        let (count_change, total_size_change) = self.stats_changes.entry(collection_name.to_string())
            .or_insert((0, 0));
        // Document count doesn't change for updates
        *total_size_change += size_change;

        Ok(())
    }

    /// Delete a document within the transaction
    pub fn delete_document(&mut self, collection_name: &str, key: &DocumentKey) -> Result<bool> {
        let _collection_info = self.engine.get_collection(collection_name)
            .ok_or_else(|| ArangoError::collection_not_found(collection_name))?;

        let doc_key = OptimisticRocksDBEngine::document_key(collection_name, key);

        // Check if document exists and get its size before deletion
        let cf = self.engine.db.cf_handle(CF_DOCUMENTS)
            .ok_or_else(|| ArangoError::internal("documents column family not found"))?;

        let db_tx = self.db_tx.as_ref()
            .ok_or_else(|| ArangoError::internal("Transaction already committed"))?;

        let document_size = match db_tx.get_cf(cf, &doc_key) {
            Ok(Some(data)) => data.len() as i64,
            Ok(None) => return Ok(false), // Document doesn't exist
            Err(e) => return Err(ArangoError::internal(format!("failed to get document for deletion: {}", e))),
        };

        let db_tx = self.db_tx.as_mut()
            .ok_or_else(|| ArangoError::internal("Transaction already committed"))?;

        db_tx.delete_cf(cf, &doc_key)
            .map_err(|e| ArangoError::internal(format!("failed to delete document: {}", e)))?;

        // Track statistics changes: -1 document, -size bytes
        let (count_change, size_change) = self.stats_changes.entry(collection_name.to_string())
            .or_insert((0, 0));
        *count_change -= 1;
        *size_change -= document_size;

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

        // First commit the database transaction
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
            })?;

        // After successful commit, update collection statistics in memory cache
        for (collection_name, (count_change, size_change)) in self.stats_changes.clone() {
            if count_change != 0 || size_change != 0 {
                if let Some(mut collection_info) = self.engine.collections.get_mut(&collection_name) {
                    // Apply the changes
                    if count_change > 0 {
                        collection_info.document_count += count_change as u64;
                    } else if count_change < 0 {
                        collection_info.document_count = collection_info.document_count.saturating_sub((-count_change) as u64);
                    }

                    if size_change > 0 {
                        collection_info.data_size += size_change as u64;
                    } else if size_change < 0 {
                        collection_info.data_size = collection_info.data_size.saturating_sub((-size_change) as u64);
                    }

                    // Update the collection metadata in storage
                    let updated_info = collection_info.clone();
                    drop(collection_info); // Release the lock

                    // Create a new transaction to update the metadata
                    let mut meta_tx = self.engine.transaction();
                    if let Err(e) = meta_tx.put_collection_metadata(&collection_name, &updated_info) {
                        // Log the error but don't fail the main transaction since data was already committed
                        eprintln!("Warning: Failed to update collection metadata: {}", e);
                    } else if let Err(e) = meta_tx.commit() {
                        eprintln!("Warning: Failed to commit metadata update: {}", e);
                    }
                }
            }
        }

        Ok(())
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

impl CollectionSchema {
    /// Create a new schema builder
    pub fn builder() -> SchemaBuilder {
        SchemaBuilder::new()
    }

    /// Validate a document against this schema
    pub fn validate_document(&self, document: &Document) -> Result<()> {
        let data = &document.data;

        // Check required fields
        for required_field in &self.required_fields {
            if !data.contains_key(required_field) {
                return Err(ArangoError::bad_parameter(
                    format!("Required field '{}' is missing", required_field)
                ));
            }
        }

        // Validate each field in the document
        for (field_name, field_value) in data {
            if let Some(field_def) = self.fields.get(field_name) {
                self.validate_field_value(field_name, field_value, field_def)?;
            } else if self.strict_mode {
                return Err(ArangoError::bad_parameter(
                    format!("Unexpected field '{}' in strict schema mode", field_name)
                ));
            }
        }

        // Set default values for missing optional fields
        // (This would require mutable access to document)

        Ok(())
    }

    /// Validate a single field value
    fn validate_field_value(&self, field_name: &str, value: &Value, field_def: &FieldDefinition) -> Result<()> {
        // Check null values
        if value.is_null() {
            if !field_def.nullable {
                return Err(ArangoError::bad_parameter(
                    format!("Field '{}' cannot be null", field_name)
                ));
            }
            return Ok(());
        }

        // Validate type
        self.validate_field_type(field_name, value, &field_def.field_type)?;

        // Validate constraints
        for constraint in &field_def.constraints {
            self.validate_constraint(field_name, value, constraint)?;
        }

        Ok(())
    }

    /// Validate field type
    fn validate_field_type(&self, field_name: &str, value: &Value, field_type: &FieldType) -> Result<()> {
        match field_type {
            FieldType::String { max_length } => {
                if !value.is_string() {
                    return Err(ArangoError::bad_parameter(
                        format!("Field '{}' must be a string", field_name)
                    ));
                }
                if let Some(max_len) = max_length {
                    let str_len = value.as_str().unwrap().len();
                    if str_len > *max_len {
                        return Err(ArangoError::bad_parameter(
                            format!("Field '{}' exceeds maximum length of {}", field_name, max_len)
                        ));
                    }
                }
            },
            FieldType::Integer => {
                if !value.is_i64() && !value.is_u64() {
                    return Err(ArangoError::bad_parameter(
                        format!("Field '{}' must be an integer", field_name)
                    ));
                }
            },
            FieldType::Float => {
                if !value.is_f64() && !value.is_i64() && !value.is_u64() {
                    return Err(ArangoError::bad_parameter(
                        format!("Field '{}' must be a number", field_name)
                    ));
                }
            },
            FieldType::Boolean => {
                if !value.is_boolean() {
                    return Err(ArangoError::bad_parameter(
                        format!("Field '{}' must be a boolean", field_name)
                    ));
                }
            },
            FieldType::Array { element_type } => {
                if !value.is_array() {
                    return Err(ArangoError::bad_parameter(
                        format!("Field '{}' must be an array", field_name)
                    ));
                }
                // Validate each element
                for (idx, element) in value.as_array().unwrap().iter().enumerate() {
                    let element_field_name = format!("{}[{}]", field_name, idx);
                    self.validate_field_type(&element_field_name, element, element_type)?;
                }
            },
            FieldType::Object { schema } => {
                if !value.is_object() {
                    return Err(ArangoError::bad_parameter(
                        format!("Field '{}' must be an object", field_name)
                    ));
                }
                // Validate nested schema if provided
                if let Some(nested_schema) = schema {
                    // For nested objects, recursively validate each field
                    if let Some(obj) = value.as_object() {
                        for (nested_field, nested_value) in obj {
                            if let Some(nested_field_def) = nested_schema.fields.get(nested_field) {
                                nested_schema.validate_field_value(nested_field, nested_value, nested_field_def)?;
                            } else if nested_schema.strict_mode {
                                return Err(ArangoError::bad_parameter(
                                    format!("Unexpected field '{}.{}' in strict schema mode", field_name, nested_field)
                                ));
                            }
                        }

                        // Check required fields in nested object
                        for required_field in &nested_schema.required_fields {
                            if !obj.contains_key(required_field) {
                                return Err(ArangoError::bad_parameter(
                                    format!("Required field '{}.{}' is missing", field_name, required_field)
                                ));
                            }
                        }
                    }
                }
            },
            FieldType::Date => {
                // Accept string (ISO 8601) or number (timestamp)
                if !value.is_string() && !value.is_number() {
                    return Err(ArangoError::bad_parameter(
                        format!("Field '{}' must be a date string or timestamp", field_name)
                    ));
                }
                // TODO: Validate date format
            },
            FieldType::Binary => {
                // Accept string (base64) or array of numbers
                if !value.is_string() && !value.is_array() {
                    return Err(ArangoError::bad_parameter(
                        format!("Field '{}' must be binary data", field_name)
                    ));
                }
            },
            FieldType::Any => {
                // No validation needed
            },
        }

        Ok(())
    }

    /// Validate field constraints
    fn validate_constraint(&self, field_name: &str, value: &Value, constraint: &FieldConstraint) -> Result<()> {
        match constraint {
            FieldConstraint::MinValue(min_val) => {
                if let Some(num_val) = value.as_f64() {
                    if num_val < *min_val {
                        return Err(ArangoError::bad_parameter(
                            format!("Field '{}' value {} is below minimum {}", field_name, num_val, min_val)
                        ));
                    }
                }
            },
            FieldConstraint::MaxValue(max_val) => {
                if let Some(num_val) = value.as_f64() {
                    if num_val > *max_val {
                        return Err(ArangoError::bad_parameter(
                            format!("Field '{}' value {} exceeds maximum {}", field_name, num_val, max_val)
                        ));
                    }
                }
            },
            FieldConstraint::MinLength(min_len) => {
                let len = if let Some(s) = value.as_str() {
                    s.len()
                } else if let Some(arr) = value.as_array() {
                    arr.len()
                } else {
                    return Ok(());
                };
                if len < *min_len {
                    return Err(ArangoError::bad_parameter(
                        format!("Field '{}' length {} is below minimum {}", field_name, len, min_len)
                    ));
                }
            },
            FieldConstraint::MaxLength(max_len) => {
                let len = if let Some(s) = value.as_str() {
                    s.len()
                } else if let Some(arr) = value.as_array() {
                    arr.len()
                } else {
                    return Ok(());
                };
                if len > *max_len {
                    return Err(ArangoError::bad_parameter(
                        format!("Field '{}' length {} exceeds maximum {}", field_name, len, max_len)
                    ));
                }
            },
            FieldConstraint::OneOf(allowed_values) => {
                if !allowed_values.contains(value) {
                    return Err(ArangoError::bad_parameter(
                        format!("Field '{}' value is not in allowed set", field_name)
                    ));
                }
            },
            FieldConstraint::Pattern(pattern) => {
                if let Some(s) = value.as_str() {
                    // TODO: Implement regex validation
                    // For now, just check if it's a string
                }
            },
            FieldConstraint::Custom(_expr) => {
                // TODO: Implement custom validation expressions
            },
        }

        Ok(())
    }
}

/// Schema builder for fluent API
pub struct SchemaBuilder {
    schema: CollectionSchema,
}

impl SchemaBuilder {
    pub fn new() -> Self {
        SchemaBuilder {
            schema: CollectionSchema::default(),
        }
    }

    /// Add a string field
    pub fn add_string_field(mut self, name: &str, max_length: Option<usize>) -> Self {
        let field_def = FieldDefinition {
            field_type: FieldType::String { max_length },
            nullable: false,
            default_value: None,
            constraints: Vec::new(),
            description: None,
        };
        self.schema.fields.insert(name.to_string(), field_def);
        self
    }

    /// Add an integer field
    pub fn add_integer_field(mut self, name: &str) -> Self {
        let field_def = FieldDefinition {
            field_type: FieldType::Integer,
            nullable: false,
            default_value: None,
            constraints: Vec::new(),
            description: None,
        };
        self.schema.fields.insert(name.to_string(), field_def);
        self
    }

    /// Add a float field
    pub fn add_float_field(mut self, name: &str) -> Self {
        let field_def = FieldDefinition {
            field_type: FieldType::Float,
            nullable: false,
            default_value: None,
            constraints: Vec::new(),
            description: None,
        };
        self.schema.fields.insert(name.to_string(), field_def);
        self
    }

    /// Add a boolean field
    pub fn add_boolean_field(mut self, name: &str) -> Self {
        let field_def = FieldDefinition {
            field_type: FieldType::Boolean,
            nullable: false,
            default_value: None,
            constraints: Vec::new(),
            description: None,
        };
        self.schema.fields.insert(name.to_string(), field_def);
        self
    }

    /// Add an array field
    pub fn add_array_field(mut self, name: &str, element_type: FieldType) -> Self {
        let field_def = FieldDefinition {
            field_type: FieldType::Array { element_type: Box::new(element_type) },
            nullable: false,
            default_value: None,
            constraints: Vec::new(),
            description: None,
        };
        self.schema.fields.insert(name.to_string(), field_def);
        self
    }

    /// Add an object field
    pub fn add_object_field(mut self, name: &str, nested_schema: Option<CollectionSchema>) -> Self {
        let field_def = FieldDefinition {
            field_type: FieldType::Object { schema: nested_schema },
            nullable: false,
            default_value: None,
            constraints: Vec::new(),
            description: None,
        };
        self.schema.fields.insert(name.to_string(), field_def);
        self
    }

    /// Mark a field as required
    pub fn require_field(mut self, name: &str) -> Self {
        self.schema.required_fields.insert(name.to_string());
        self
    }

    /// Set strict mode
    pub fn strict_mode(mut self, strict: bool) -> Self {
        self.schema.strict_mode = strict;
        self
    }

    /// Build the schema
    pub fn build(self) -> CollectionSchema {
        self.schema
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
            counter_doc.set("counter".to_string(), json!(current + 1));
            tx.update_document("test_conflict", &counter_doc).unwrap();
        }

        let result = tx.commit();
        assert!(result.is_ok());
    }
} 