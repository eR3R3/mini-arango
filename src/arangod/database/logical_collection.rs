use std::sync::Arc;
use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use crate::common::error::{ArangoError, Result};
use crate::common::document::{Document, DocumentKey, DocumentFilter};
use super::vocbase_types::*;
use super::vocbase::VocBase;
use super::validators::{ValidatorBase, ValidatorFactory, ValidationLevel};
use super::computed_values::{ComputedValues, ComputeValuesOn, build_computed_values_instance};

/// Collection version for compatibility tracking
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[repr(u32)]
pub enum CollectionVersion {
    V30 = 5,
    V31 = 6,
    V33 = 7,
    V34 = 8,
    V37 = 9,
}

impl CollectionVersion {
    pub fn minimum() -> Self {
        CollectionVersion::V30
    }
    
    pub fn current() -> Self {
        CollectionVersion::V37
    }
    
    pub fn from_u32(value: u32) -> Self {
        match value {
            5 => CollectionVersion::V30,
            6 => CollectionVersion::V31,
            7 => CollectionVersion::V33,
            8 => CollectionVersion::V34,
            9 => CollectionVersion::V37,
            _ => CollectionVersion::current(),
        }
    }
    
    pub fn as_u32(&self) -> u32 {
        *self as u32
    }
}

/// Collection status enumeration
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CollectionStatus {
    NewBorn,
    Unloaded,
    Loaded,
    BeingUnloaded,
    Deleted,
    Loading,
}

impl Default for CollectionStatus {
    fn default() -> Self {
        CollectionStatus::NewBorn
    }
}

/// User input collection properties (simplified without clustering)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UserInputCollectionProperties {
    pub name: String,
    pub id: DataSourceId,
    pub wait_for_sync: bool,
    pub cache_enabled: bool,
    pub schema: Option<JsonValue>,
    pub computed_values: Option<JsonValue>,
}

impl Default for UserInputCollectionProperties {
    fn default() -> Self {
        UserInputCollectionProperties {
            name: String::new(),
            id: DataSourceId::new(0),
            wait_for_sync: false,
            cache_enabled: true,
            schema: None,
            computed_values: None,
        }
    }
}

/// Key generator for document keys
#[derive(Debug, Clone)]
pub struct KeyGenerator {
    pub allow_user_keys: bool,
    pub key_increment: Arc<RwLock<u64>>,
}

impl KeyGenerator {
    pub fn new(allow_user_keys: bool) -> Self {
        KeyGenerator {
            allow_user_keys,
            key_increment: Arc::new(RwLock::new(1)),
        }
    }
    
    pub fn generate_key(&self) -> DocumentKey {
        let mut increment = self.key_increment.write();
        let key = format!("{}", *increment);
        *increment += 1;
        DocumentKey::new(key).unwrap()
    }
    
    pub fn validate_key(&self, key: &str) -> Result<()> {
        if key.is_empty() {
            return Err(ArangoError::bad_parameter("document key cannot be empty"));
        }
        
        if key.len() > 254 {
            return Err(ArangoError::bad_parameter("document key too long"));
        }
        
        // System keys start with underscore and are only allowed for system collections
        if key.starts_with('_') && !self.allow_user_keys {
            return Err(ArangoError::bad_parameter("user keys cannot start with underscore"));
        }
        
        // Check for valid characters (simplified)
        if !key.chars().all(|c| c.is_alphanumeric() || c == '_' || c == '-') {
            return Err(ArangoError::bad_parameter("document key contains invalid characters"));
        }
        
        Ok(())
    }
}

impl Default for KeyGenerator {
    fn default() -> Self {
        Self::new(true)
    }
}

/// Physical collection interface - this would be implemented by storage engines
pub trait PhysicalCollection: Send + Sync {
    fn insert_document(&self, document: &Document) -> Result<()>;
    fn update_document(&self, document: &Document) -> Result<()>;
    fn remove_document(&self, key: &DocumentKey) -> Result<bool>;
    fn get_document(&self, key: &DocumentKey) -> Result<Option<Document>>;
    fn count_documents(&self) -> Result<u64>;
    fn truncate(&self) -> Result<()>;
    fn cache_enabled(&self) -> bool;
    fn figures(&self) -> Result<CollectionFigures>;
}

/// Collection statistics/figures
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CollectionFigures {
    pub alive: AliveDocumentStats,
    pub dead: DeadDocumentStats,
    pub indexes: IndexStats,
    pub read_cache: CacheStats,
    pub cache_usage: u64,
    pub cache_size: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AliveDocumentStats {
    pub count: u64,
    pub size: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeadDocumentStats {
    pub count: u64,
    pub size: u64,
    pub deletion: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexStats {
    pub count: u64,
    pub size: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CacheStats {
    pub in_use: u64,
    pub hit_rate: f64,
    pub hits: u64,
    pub misses: u64,
}

/// Main LogicalCollection struct - simplified version without clustering
pub struct LogicalCollection {
    // Meta information
    version: CollectionVersion,
    v8_cache_version: u32,
    collection_type: CollectionType,
    
    // Identifiers
    id: DataSourceId,
    plan_id: DataSourceId,
    name: String,
    guid: String,
    
    // Properties
    allow_user_keys: bool,
    uses_revisions_as_document_ids: bool,
    wait_for_sync: bool,
    cache_enabled: bool,
    deleted: bool,
    system: bool,
    
    // Components
    key_generator: Arc<KeyGenerator>,
    physical: Option<Arc<dyn PhysicalCollection>>,
    
    // VocBase reference
    vocbase: Arc<VocBase>,
    
    // Status and synchronization
    status: Arc<RwLock<CollectionStatus>>,
    
    // Count cache for performance
    count_cache_ttl: f64,
    cached_count: Arc<RwLock<Option<(u64, SystemTime)>>>,
    
    // Schema validation
    schema_validator: Arc<RwLock<Option<Arc<dyn ValidatorBase>>>>,
    internal_validators: Vec<Arc<dyn ValidatorBase>>,
    internal_validator_types: u64,
    
    // Computed values
    computed_values: Arc<RwLock<Option<ComputedValues>>>,
}

impl LogicalCollection {
    /// Create a new logical collection
    pub fn new(
        vocbase: Arc<VocBase>,
        info: &JsonValue,
        physical: Option<Arc<dyn PhysicalCollection>>,
    ) -> Result<Self> {
        let id = DataSourceId::new(
            info.get("id")
                .and_then(|v| v.as_u64())
                .unwrap_or_else(|| Self::generate_id())
        );
        
        let plan_id = DataSourceId::new(
            info.get("planId")
                .and_then(|v| v.as_u64())
                .unwrap_or(id.id())
        );
        
        let name = info.get("name")
            .and_then(|v| v.as_str())
            .ok_or_else(|| ArangoError::bad_parameter("collection name is required"))?
            .to_string();
        
        let guid = info.get("globallyUniqueId")
            .and_then(|v| v.as_str())
            .unwrap_or(&name)
            .to_string();
        
        let version = CollectionVersion::from_u32(
            info.get("version")
                .and_then(|v| v.as_u64())
                .unwrap_or(CollectionVersion::current().as_u32() as u64) as u32
        );
        
        if version < CollectionVersion::minimum() {
            return Err(ArangoError::bad_parameter(
                format!("collection '{}' has a too old version. Please upgrade the database.", name)
            ));
        }
        
        let collection_type = CollectionType::from_u32(
            info.get("type")
                .and_then(|v| v.as_u64())
                .unwrap_or(CollectionType::Document.as_u32() as u64) as u32
        );
        
        let allow_user_keys = info.get("keyOptions")
            .and_then(|ko| ko.get("allowUserKeys"))
            .and_then(|v| v.as_bool())
            .unwrap_or(true);
        
        let wait_for_sync = info.get("waitForSync")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        
        let cache_enabled = info.get("cacheEnabled")
            .and_then(|v| v.as_bool())
            .unwrap_or(true);
        
        let system = info.get("isSystem")
            .and_then(|v| v.as_bool())
            .unwrap_or(name.starts_with('_'));
        
        let uses_revisions_as_document_ids = info.get("usesRevisionsAsDocumentIds")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        
        let key_generator = Arc::new(KeyGenerator::new(allow_user_keys));
        
        let mut collection = LogicalCollection {
            version,
            v8_cache_version: 0,
            collection_type,
            id,
            plan_id,
            name,
            guid,
            allow_user_keys,
            uses_revisions_as_document_ids,
            wait_for_sync,
            cache_enabled,
            deleted: false,
            system,
            key_generator,
            physical,
            vocbase,
            status: Arc::new(RwLock::new(CollectionStatus::NewBorn)),
            count_cache_ttl: if system { 900.0 } else { 180.0 },
            cached_count: Arc::new(RwLock::new(None)),
            schema_validator: Arc::new(RwLock::new(None)),
            internal_validators: Vec::new(),
            internal_validator_types: 0,
            computed_values: Arc::new(RwLock::new(None)),
        };
        
        // Initialize schema validation
        if let Some(schema) = info.get("schema") {
            collection.update_schema(schema)?;
        }
        
        // Initialize computed values
        if let Some(computed_values) = info.get("computedValues") {
            collection.update_computed_values(computed_values)?;
        }
        
        // Initialize internal validator types
        collection.internal_validator_types = info.get("internalValidatorTypes")
            .and_then(|v| v.as_u64())
            .unwrap_or(0);
        
        Ok(collection)
    }
    
    fn generate_id() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64
    }
    
    // Getters for basic properties
    pub fn id(&self) -> &DataSourceId {
        &self.id
    }
    
    pub fn plan_id(&self) -> &DataSourceId {
        &self.plan_id
    }
    
    pub fn name(&self) -> &str {
        &self.name
    }
    
    pub fn guid(&self) -> &str {
        &self.guid
    }
    
    pub fn version(&self) -> CollectionVersion {
        self.version
    }
    
    pub fn collection_type(&self) -> CollectionType {
        self.collection_type
    }
    
    pub fn wait_for_sync(&self) -> bool {
        self.wait_for_sync
    }
    
    pub fn cache_enabled(&self) -> bool {
        self.cache_enabled
    }
    
    pub fn allow_user_keys(&self) -> bool {
        self.allow_user_keys
    }
    
    pub fn uses_revisions_as_document_ids(&self) -> bool {
        self.uses_revisions_as_document_ids
    }
    
    pub fn is_system(&self) -> bool {
        self.system
    }
    
    pub fn is_deleted(&self) -> bool {
        self.deleted
    }
    
    pub fn status(&self) -> CollectionStatus {
        *self.status.read()
    }
    
    // Key generation
    pub fn key_generator(&self) -> &Arc<KeyGenerator> {
        &self.key_generator
    }
    
    pub fn new_revision_id(&self) -> RevisionId {
        // Simplified - in real ArangoDB this might check for cluster-wide unique revisions
        RevisionId::create()
    }
    
    // Schema validation methods
    pub fn update_schema(&mut self, schema: &JsonValue) -> Result<()> {
        if schema.is_null() {
            *self.schema_validator.write() = None;
            return Ok(());
        }
        
        if !schema.is_object() {
            return Err(ArangoError::bad_parameter("Schema description is not an object"));
        }
        
        if schema.as_object().unwrap().is_empty() {
            // Empty object removes schema
            *self.schema_validator.write() = None;
        } else {
            // Create new schema validator
            let validator = ValidatorFactory::create_json_schema_validator(schema, ValidationLevel::Strict)?;
            *self.schema_validator.write() = Some(validator);
        }
        
        Ok(())
    }
    
    pub fn schema(&self) -> Option<Arc<dyn ValidatorBase>> {
        self.schema_validator.read().clone()
    }
    
    pub fn schema_to_json(&self) -> JsonValue {
        if let Some(validator) = self.schema() {
            validator.to_json()
        } else {
            JsonValue::Null
        }
    }
    
    // Computed values methods
    pub fn update_computed_values(&mut self, computed_values: &JsonValue) -> Result<()> {
        if computed_values.is_null() {
            *self.computed_values.write() = None;
            return Ok(());
        }
        
        // For simplified implementation, we don't have shard keys, so pass empty slice
        let empty_shard_keys: &[String] = &[];
        let computed_values_obj = build_computed_values_instance(empty_shard_keys, computed_values)?;
        *self.computed_values.write() = computed_values_obj;
        
        Ok(())
    }
    
    pub fn computed_values(&self) -> Option<ComputedValues> {
        self.computed_values.read().clone()
    }
    
    pub fn computed_values_to_json(&self) -> JsonValue {
        if let Some(cv) = self.computed_values() {
            cv.to_json()
        } else {
            JsonValue::Null
        }
    }
    
    // Internal validators
    pub fn set_internal_validator_types(&mut self, types: u64) {
        self.internal_validator_types = types;
    }
    
    pub fn get_internal_validator_types(&self) -> u64 {
        self.internal_validator_types
    }
    
    pub fn add_internal_validator(&mut self, validator: Arc<dyn ValidatorBase>) {
        self.internal_validators.push(validator);
    }
    
    // Document validation
    pub fn validate_document(&self, new_doc: &JsonValue, old_doc: Option<&JsonValue>, is_insert: bool) -> Result<()> {
        // Schema validation
        if let Some(schema_validator) = self.schema() {
            schema_validator.validate(new_doc, old_doc, is_insert)?;
        }
        
        // Internal validators
        for validator in &self.internal_validators {
            validator.validate(new_doc, old_doc, is_insert)?;
        }
        
        Ok(())
    }
    
    // Document operations (delegated to physical collection with validation)
    pub fn insert_document(&self, mut document: Document) -> Result<()> {
        // Apply computed values
        if let Some(cv) = self.computed_values() {
            if cv.must_compute_values_on_insert() {
                let keys_written = std::collections::HashSet::new(); // For inserts, no keys are pre-written
                let document_json = serde_json::Value::Object(document.data.clone());
                let computed_doc = cv.merge_computed_attributes(
                    &document_json,
                    &keys_written,
                    ComputeValuesOn::Insert,
                )?;
                document = Document::from_json(&self.name, computed_doc)?;
            }
        }
        
        // Validate document
        let document_json = serde_json::Value::Object(document.data.clone());
        self.validate_document(&document_json, None, true)?;
        
        if let Some(ref physical) = self.physical {
            self.invalidate_count_cache();
            physical.insert_document(&document)
        } else {
            Err(ArangoError::internal("No physical collection available"))
        }
    }
    
    pub fn update_document(&self, new_document: Document, old_document: Option<&Document>) -> Result<()> {
        // Apply computed values
        let mut final_document = new_document;
        if let Some(cv) = self.computed_values() {
            if cv.must_compute_values_on_update() {
                // For updates, we need to know which keys were written in the update
                // For simplicity, assume all top-level keys in new_document were written
                let keys_written: std::collections::HashSet<String> = final_document.data.keys().cloned().collect();
                
                let document_json = serde_json::Value::Object(final_document.data.clone());
                let computed_doc = cv.merge_computed_attributes(
                    &document_json,
                    &keys_written,
                    ComputeValuesOn::Update,
                )?;
                final_document = Document::from_json(&self.name, computed_doc)?;
            }
        }
        
        // Validate document
        let final_document_json = serde_json::Value::Object(final_document.data.clone());
        let old_doc_data = old_document.map(|d| serde_json::Value::Object(d.data.clone()));
        self.validate_document(&final_document_json, old_doc_data.as_ref(), false)?;
        
        if let Some(ref physical) = self.physical {
            physical.update_document(&final_document)
        } else {
            Err(ArangoError::internal("No physical collection available"))
        }
    }
    
    pub fn replace_document(&self, new_document: Document, old_document: Option<&Document>) -> Result<()> {
        // Apply computed values
        let mut final_document = new_document;
        if let Some(cv) = self.computed_values() {
            if cv.must_compute_values_on_replace() {
                let keys_written = std::collections::HashSet::new(); // For replaces, assume no keys pre-written
                let document_json = serde_json::Value::Object(final_document.data.clone());
                let computed_doc = cv.merge_computed_attributes(
                    &document_json,
                    &keys_written,
                    ComputeValuesOn::Replace,
                )?;
                final_document = Document::from_json(&self.name, computed_doc)?;
            }
        }
        
        // Validate document
        let final_document_json = serde_json::Value::Object(final_document.data.clone());
        let old_doc_data = old_document.map(|d| serde_json::Value::Object(d.data.clone()));
        self.validate_document(&final_document_json, old_doc_data.as_ref(), false)?;
        
        if let Some(ref physical) = self.physical {
            physical.update_document(&final_document)
        } else {
            Err(ArangoError::internal("No physical collection available"))
        }
    }
    
    pub fn remove_document(&self, key: &DocumentKey) -> Result<bool> {
        if let Some(ref physical) = self.physical {
            let result = physical.remove_document(key);
            if result.is_ok() {
                self.invalidate_count_cache();
            }
            result
        } else {
            Err(ArangoError::internal("No physical collection available"))
        }
    }
    
    pub fn get_document(&self, key: &DocumentKey) -> Result<Option<Document>> {
        if let Some(ref physical) = self.physical {
            physical.get_document(key)
        } else {
            Err(ArangoError::internal("No physical collection available"))
        }
    }
    
    pub fn count_documents(&self) -> Result<u64> {
        // Check cache first
        {
            let cache = self.cached_count.read();
            if let Some((count, timestamp)) = *cache {
                let now = SystemTime::now();
                if let Ok(duration) = now.duration_since(timestamp) {
                    if duration.as_secs_f64() < self.count_cache_ttl {
                        return Ok(count);
                    }
                }
            }
        }
        
        // Cache miss or expired, get from physical collection
        if let Some(ref physical) = self.physical {
            let count = physical.count_documents()?;
            
            // Update cache
            {
                let mut cache = self.cached_count.write();
                *cache = Some((count, SystemTime::now()));
            }
            
            Ok(count)
        } else {
            Err(ArangoError::internal("No physical collection available"))
        }
    }
    
    pub fn truncate(&self) -> Result<()> {
        if let Some(ref physical) = self.physical {
            self.invalidate_count_cache();
            physical.truncate()
        } else {
            Err(ArangoError::internal("No physical collection available"))
        }
    }
    
    pub fn figures(&self) -> Result<CollectionFigures> {
        if let Some(ref physical) = self.physical {
            physical.figures()
        } else {
            Err(ArangoError::internal("No physical collection available"))
        }
    }
    
    // Collection lifecycle
    pub fn load(&self) -> Result<()> {
        let mut status = self.status.write();
        match *status {
            CollectionStatus::Unloaded => {
                *status = CollectionStatus::Loading;
                // Perform loading operations here
                *status = CollectionStatus::Loaded;
                Ok(())
            }
            CollectionStatus::Loaded => Ok(()),
            _ => Err(ArangoError::internal("Cannot load collection in current state"))
        }
    }
    
    pub fn unload(&self) -> Result<()> {
        let mut status = self.status.write();
        match *status {
            CollectionStatus::Loaded => {
                *status = CollectionStatus::BeingUnloaded;
                // Perform unloading operations here
                *status = CollectionStatus::Unloaded;
                Ok(())
            }
            CollectionStatus::Unloaded => Ok(()),
            _ => Err(ArangoError::internal("Cannot unload collection in current state"))
        }
    }
    
    pub fn drop(&self) -> Result<()> {
        let mut status = self.status.write();
        *status = CollectionStatus::Deleted;
        // Perform cleanup operations here
        Ok(())
    }
    
    /// Cleanup collection resources
    pub fn cleanup(&self) -> Result<()> {
        let mut status = self.status.write();
        *status = CollectionStatus::Deleted;
        
        // Clear any cached data
        self.invalidate_count_cache();
        
        // In a real implementation, this would:
        // - Close any open files
        // - Release memory
        // - Clean up indexes
        // - Notify dependent components
        
        Ok(())
    }
    
    // Properties that can be updated
    pub fn update_properties(&mut self, properties: &JsonValue) -> Result<()> {
        if let Some(wait_for_sync) = properties.get("waitForSync").and_then(|v| v.as_bool()) {
            self.wait_for_sync = wait_for_sync;
        }
        
        if let Some(cache_enabled) = properties.get("cacheEnabled").and_then(|v| v.as_bool()) {
            self.cache_enabled = cache_enabled;
        }
        
        // Update schema if provided
        if let Some(schema) = properties.get("schema") {
            self.update_schema(schema)?;
        }
        
        // Update computed values if provided
        if let Some(computed_values) = properties.get("computedValues") {
            self.update_computed_values(computed_values)?;
        }
        
        Ok(())
    }
    
    // Collection properties for API responses
    pub fn get_collection_properties(&self) -> UserInputCollectionProperties {
        UserInputCollectionProperties {
            name: self.name.clone(),
            id: self.id.clone(),
            wait_for_sync: self.wait_for_sync,
            cache_enabled: self.cache_enabled,
            schema: if self.schema().is_some() {
                Some(self.schema_to_json())
            } else {
                None
            },
            computed_values: if self.computed_values().is_some() {
                Some(self.computed_values_to_json())
            } else {
                None
            },
        }
    }
    
    // Serialization for API responses
    pub fn to_velocypack(&self) -> JsonValue {
        let mut result = serde_json::json!({
            "id": self.id.id().to_string(),
            "name": self.name,
            "type": self.collection_type.as_u32(),
            "status": self.status() as u32,
            "waitForSync": self.wait_for_sync,
            "cacheEnabled": self.cache_enabled,
            "system": self.system,
            "version": self.version.as_u32(),
            "globallyUniqueId": self.guid,
            "planId": self.plan_id.id().to_string(),
        });
        
        // Add schema if present
        if self.schema().is_some() {
            result["schema"] = self.schema_to_json();
        }
        
        // Add computed values if present
        if self.computed_values().is_some() {
            result["computedValues"] = self.computed_values_to_json();
        }
        
        result
    }
    
    fn invalidate_count_cache(&self) {
        let mut cache = self.cached_count.write();
        *cache = None;
    }
} 