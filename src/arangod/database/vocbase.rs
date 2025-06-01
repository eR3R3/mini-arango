use std::sync::Arc;
use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use crate::common::error::{ArangoError, Result};
use crate::common::document::{Document, DocumentKey};
use super::vocbase_types::*;
use super::logical_collection::{LogicalCollection, PhysicalCollection};

/// Database/VocBase information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VocBaseInfo {
    pub id: DataSourceId,
    pub name: String,
    pub path: String,
    pub is_system: bool,
    pub created: SystemTime,
}

impl VocBaseInfo {
    pub fn new(id: DataSourceId, name: String, is_system: bool) -> Self {
        VocBaseInfo {
            id,
            name: name.clone(),
            path: format!("databases/{}", name),
            is_system,
            created: SystemTime::now(),
        }
    }
}

/// Database statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VocBaseStats {
    pub collections: usize,
    pub documents: u64,
    pub data_size: u64,
    pub index_size: u64,
    pub cache_size: u64,
}

impl Default for VocBaseStats {
    fn default() -> Self {
        VocBaseStats {
            collections: 0,
            documents: 0,
            data_size: 0,
            index_size: 0,
            cache_size: 0,
        }
    }
}

/// Health status of the database
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum HealthStatus {
    Good,
    Bad,
    Unknown,
}

impl Default for HealthStatus {
    fn default() -> Self {
        HealthStatus::Good
    }
}

/// VocBase (database) manages collections and provides database-level operations
pub struct VocBase {
    // Basic information
    info: VocBaseInfo,
    
    // Collections management
    collections: Arc<RwLock<HashMap<String, Arc<LogicalCollection>>>>,
    collections_by_id: Arc<RwLock<HashMap<DataSourceId, Arc<LogicalCollection>>>>,
    
    // Status and metadata
    deleted: Arc<RwLock<bool>>,
    stats: Arc<RwLock<VocBaseStats>>,
    
    // Global tick for this database
    tick: Arc<RwLock<VocTick>>,
}

impl VocBase {
    /// Create a new VocBase (database)
    pub fn new(name: String, is_system: bool) -> Result<Self> {
        // Validate database name
        Self::validate_database_name(&name)?;
        
        let id = DataSourceId::new(Self::generate_id());
        let info = VocBaseInfo::new(id, name, is_system);
        
        Ok(VocBase {
            info,
            collections: Arc::new(RwLock::new(HashMap::new())),
            collections_by_id: Arc::new(RwLock::new(HashMap::new())),
            deleted: Arc::new(RwLock::new(false)),
            stats: Arc::new(RwLock::new(VocBaseStats::default())),
            tick: Arc::new(RwLock::new(1)),
        })
    }
    
    fn generate_id() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64
    }
    
    fn validate_database_name(name: &str) -> Result<()> {
        if name.is_empty() {
            return Err(ArangoError::bad_parameter("database name cannot be empty"));
        }
        
        if name.len() > 64 {
            return Err(ArangoError::bad_parameter("database name too long (max 64 characters)"));
        }
        
        // Check for valid characters
        if !name.chars().all(|c| c.is_alphanumeric() || c == '_' || c == '-') {
            return Err(ArangoError::bad_parameter("database name contains invalid characters"));
        }
        
        // System database is special
        if name == "_system" {
            return Ok(());
        }
        
        // User databases cannot start with underscore
        if name.starts_with('_') {
            return Err(ArangoError::bad_parameter("user database names cannot start with underscore"));
        }
        
        Ok(())
    }
    
    // Basic getters
    pub fn id(&self) -> &DataSourceId {
        &self.info.id
    }
    
    pub fn name(&self) -> &str {
        &self.info.name
    }
    
    pub fn info(&self) -> &VocBaseInfo {
        &self.info
    }
    
    pub fn is_system(&self) -> bool {
        self.info.is_system
    }
    
    pub fn is_deleted(&self) -> bool {
        *self.deleted.read()
    }
    
    pub fn path(&self) -> &str {
        &self.info.path
    }
    
    /// Generate a new tick for this database
    pub fn new_tick(&self) -> VocTick {
        let mut tick = self.tick.write();
        *tick += 1;
        *tick
    }
    
    /// Get current tick
    pub fn current_tick(&self) -> VocTick {
        *self.tick.read()
    }
    
    // Collection management
    
    /// Create a new collection
    pub fn create_collection(
        &self,
        name: &str,
        collection_type: CollectionType,
        properties: Option<&JsonValue>,
        physical: Option<Arc<dyn PhysicalCollection>>,
    ) -> Result<Arc<LogicalCollection>> {
        // Validate collection name
        Self::validate_collection_name(name)?;
        
        // Check if collection already exists
        {
            let collections = self.collections.read();
            if collections.contains_key(name) {
                return Err(ArangoError::bad_parameter(
                    format!("collection '{}' already exists", name)
                ));
            }
        }
        
        // Build collection info
        let mut collection_info = serde_json::json!({
            "name": name,
            "type": collection_type.as_u32(),
            "isSystem": name.starts_with('_'),
            "waitForSync": false,
            "cacheEnabled": true,
        });
        
        // Merge with user properties
        if let Some(props) = properties {
            if let Some(props_obj) = props.as_object() {
                if let Some(info_obj) = collection_info.as_object_mut() {
                    for (key, value) in props_obj {
                        info_obj.insert(key.clone(), value.clone());
                    }
                }
            }
        }
        
        // Create logical collection - need to create a shared reference to self
        let self_arc = unsafe {
            // This is a bit of a hack to get an Arc<Self> from &self
            // In a real implementation, this would be handled differently
            Arc::from_raw(self as *const Self)
        };
        
        let logical_collection = Arc::new(LogicalCollection::new(
            self_arc,
            &collection_info,
            physical,
        )?);
        
        // Add to collections maps
        {
            let mut collections = self.collections.write();
            let mut collections_by_id = self.collections_by_id.write();
            
            collections.insert(name.to_string(), Arc::clone(&logical_collection));
            collections_by_id.insert(logical_collection.id().clone(), Arc::clone(&logical_collection));
        }
        
        // Update stats
        self.update_stats();
        
        Ok(logical_collection)
    }
    
    /// Drop a collection
    pub fn drop_collection(&self, name: &str) -> Result<()> {
        let collection = {
            let mut collections = self.collections.write();
            collections.remove(name)
        };
        
        if let Some(collection) = collection {
            // Remove from ID map
            {
                let mut collections_by_id = self.collections_by_id.write();
                collections_by_id.remove(collection.id());
            }
            
            // Cleanup the collection (instead of calling drop)
            collection.cleanup()?;
            
            // Update stats
            self.update_stats();
            
            Ok(())
        } else {
            Err(ArangoError::not_found(format!("collection '{}' not found", name)))
        }
    }
    
    /// Get a collection by name
    pub fn get_collection(&self, name: &str) -> Option<Arc<LogicalCollection>> {
        let collections = self.collections.read();
        collections.get(name).cloned()
    }
    
    /// Get a collection by ID
    pub fn get_collection_by_id(&self, id: &DataSourceId) -> Option<Arc<LogicalCollection>> {
        let collections_by_id = self.collections_by_id.read();
        collections_by_id.get(id).cloned()
    }
    
    /// List all collection names
    pub fn list_collection_names(&self) -> Vec<String> {
        let collections = self.collections.read();
        collections.keys().cloned().collect()
    }
    
    /// List all collections
    pub fn list_collections(&self) -> Vec<Arc<LogicalCollection>> {
        let collections = self.collections.read();
        collections.values().cloned().collect()
    }
    
    /// Check if collection exists
    pub fn has_collection(&self, name: &str) -> bool {
        let collections = self.collections.read();
        collections.contains_key(name)
    }
    
    /// Count collections
    pub fn collection_count(&self) -> usize {
        let collections = self.collections.read();
        collections.len()
    }
    
    /// Rename a collection
    pub fn rename_collection(&self, old_name: &str, new_name: &str) -> Result<()> {
        // Validate new name
        Self::validate_collection_name(new_name)?;
        
        let collection = {
            let mut collections = self.collections.write();
            
            // Check if new name already exists
            if collections.contains_key(new_name) {
                return Err(ArangoError::bad_parameter(
                    format!("collection '{}' already exists", new_name)
                ));
            }
            
            // Remove with old name
            let collection = collections.remove(old_name)
                .ok_or_else(|| ArangoError::not_found(format!("collection '{}' not found", old_name)))?;
            
            // Re-insert with new name
            collections.insert(new_name.to_string(), Arc::clone(&collection));
            
            collection
        };
        
        // Note: In real ArangoDB, this would update the collection's internal name
        // For our simplified version, we just update the mapping
        
        Ok(())
    }
    
    fn validate_collection_name(name: &str) -> Result<()> {
        if name.is_empty() {
            return Err(ArangoError::bad_parameter("collection name cannot be empty"));
        }
        
        if name.len() > 256 {
            return Err(ArangoError::bad_parameter("collection name too long"));
        }
        
        // Check for valid characters (more restrictive than database names)
        let first_char = name.chars().next().unwrap();
        if !first_char.is_alphabetic() && first_char != '_' {
            return Err(ArangoError::bad_parameter("collection name must start with letter or underscore"));
        }
        
        if !name.chars().all(|c| c.is_alphanumeric() || c == '_') {
            return Err(ArangoError::bad_parameter("collection name contains invalid characters"));
        }
        
        Ok(())
    }
    
    // Statistics and health
    
    /// Get database statistics
    pub fn get_stats(&self) -> VocBaseStats {
        self.stats.read().clone()
    }
    
    /// Update database statistics by scanning collections
    pub fn update_stats(&self) {
        let collections = self.collections.read();
        
        let mut total_documents = 0u64;
        let mut total_data_size = 0u64;
        let mut total_index_size = 0u64;
        let mut total_cache_size = 0u64;
        
        for collection in collections.values() {
            if let Ok(count) = collection.count_documents() {
                total_documents += count;
            }
            
            if let Ok(figures) = collection.figures() {
                total_data_size += figures.alive.size;
                total_index_size += figures.indexes.size;
                total_cache_size += figures.cache_size;
            }
        }
        
        let mut stats = self.stats.write();
        stats.collections = collections.len();
        stats.documents = total_documents;
        stats.data_size = total_data_size;
        stats.index_size = total_index_size;
        stats.cache_size = total_cache_size;
    }
    
    /// Get health status
    pub fn health(&self) -> HealthStatus {
        if self.is_deleted() {
            HealthStatus::Bad
        } else {
            HealthStatus::Good
        }
    }
    
    // Database lifecycle
    
    /// Mark database as deleted
    pub fn mark_deleted(&self) {
        let mut deleted = self.deleted.write();
        *deleted = true;
    }
    
    /// Drop all collections and clean up
    pub fn cleanup(&self) -> Result<()> {
        // Get all collection names
        let collection_names: Vec<String> = {
            let collections = self.collections.read();
            collections.keys().cloned().collect()
        };
        
        // Drop all collections
        for name in collection_names {
            self.drop_collection(&name)?;
        }
        
        // Mark as deleted
        self.mark_deleted();
        
        Ok(())
    }
    
    // Validation methods
    pub fn validate_collection_parameters(&self, parameters: &JsonValue) -> Result<()> {
        if !parameters.is_object() {
            return Err(ArangoError::bad_parameter("collection parameters should be an object"));
        }
        
        // Check that the name is valid
        let name = parameters.get("name")
            .and_then(|n| n.as_str())
            .unwrap_or("");
        let is_system = parameters.get("isSystem")
            .and_then(|s| s.as_bool())
            .unwrap_or(false);
            
        Self::validate_collection_name(name)?;
        
        let collection_type = parameters.get("type")
            .and_then(|t| t.as_u64())
            .unwrap_or(CollectionType::Document.as_u32() as u64) as u32;
            
        let collection_type = CollectionType::from_u32(collection_type);
        
        if collection_type == CollectionType::Unknown {
            return Err(ArangoError::bad_parameter(
                format!("invalid collection type for collection '{}'", name)
            ));
        }
        
        Ok(())
    }
    
    // Serialization for API responses
    pub fn to_velocypack(&self) -> JsonValue {
        serde_json::json!({
            "id": self.info.id.id().to_string(),
            "name": self.info.name,
            "path": self.info.path,
            "isSystem": self.info.is_system,
        })
    }
    
    pub fn to_velocypack_with_stats(&self) -> JsonValue {
        let stats = self.get_stats();
        serde_json::json!({
            "id": self.info.id.id().to_string(),
            "name": self.info.name,
            "path": self.info.path,
            "isSystem": self.info.is_system,
            "stats": {
                "collections": stats.collections,
                "documents": stats.documents,
                "dataSize": stats.data_size,
                "indexSize": stats.index_size,
                "cacheSize": stats.cache_size,
            }
        })
    }
}

// Note: This Clone implementation is simplified for the example
// In practice, VocBase would likely not be Clone due to its complex state
impl Clone for VocBase {
    fn clone(&self) -> Self {
        VocBase {
            info: self.info.clone(),
            collections: Arc::new(RwLock::new(HashMap::new())),
            collections_by_id: Arc::new(RwLock::new(HashMap::new())),
            deleted: Arc::new(RwLock::new(*self.deleted.read())),
            stats: Arc::new(RwLock::new(self.stats.read().clone())),
            tick: Arc::new(RwLock::new(*self.tick.read())),
        }
    }
}

/// VocBase manager for handling multiple databases
pub struct VocBaseManager {
    databases: Arc<RwLock<HashMap<String, Arc<VocBase>>>>,
}

impl VocBaseManager {
    pub fn new() -> Self {
        VocBaseManager {
            databases: Arc::new(RwLock::new(HashMap::new())),
        }
    }
    
    /// Create a new database
    pub fn create_database(&self, name: String, is_system: bool) -> Result<Arc<VocBase>> {
        let vocbase = Arc::new(VocBase::new(name.clone(), is_system)?);
        
        let mut databases = self.databases.write();
        if databases.contains_key(&name) {
            return Err(ArangoError::bad_parameter(
                format!("database '{}' already exists", name)
            ));
        }
        
        databases.insert(name, Arc::clone(&vocbase));
        Ok(vocbase)
    }
    
    /// Get a database by name
    pub fn get_database(&self, name: &str) -> Option<Arc<VocBase>> {
        let databases = self.databases.read();
        databases.get(name).cloned()
    }
    
    /// Drop a database
    pub fn drop_database(&self, name: &str) -> Result<()> {
        let vocbase = {
            let mut databases = self.databases.write();
            databases.remove(name)
        };
        
        if let Some(vocbase) = vocbase {
            vocbase.cleanup()?;
            Ok(())
        } else {
            Err(ArangoError::not_found(format!("database '{}' not found", name)))
        }
    }
    
    /// List all database names
    pub fn list_database_names(&self) -> Vec<String> {
        let databases = self.databases.read();
        databases.keys().cloned().collect()
    }
    
    /// List all databases
    pub fn list_databases(&self) -> Vec<Arc<VocBase>> {
        let databases = self.databases.read();
        databases.values().cloned().collect()
    }
}

impl Default for VocBaseManager {
    fn default() -> Self {
        Self::new()
    }
} 