use std::sync::Arc;
use std::collections::HashMap;
use std::path::Path;
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use serde_json::{Value, Map};
use crate::common::error::{ArangoError, Result};
use crate::common::document::{Document, DocumentKey};
use crate::arangod::database::collection::{Collection, CollectionInfo};
use crate::arangod::storage_engine::{StorageEngine, CollectionType, OptimisticRocksDBEngine};
use crate::arangod::transaction::{TransactionManager, TransactionMode, TransactionOptions, SimpleTransactionMethods};
use crate::arangod::indexes::{IndexDefinition, IndexType};

/// Database implementation
pub struct Database {
    /// Database name
    name: String,
    /// Storage engine
    storage: Arc<dyn StorageEngine>,
}

/// Database information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatabaseInfo {
    pub name: String,
    pub id: String,
    pub path: String,
    pub is_system: bool,
}

/// Database statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatabaseStats {
    pub collections: usize,
    pub documents: u64,
    pub data_size: u64,
}

/// Health status of the database
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum HealthStatus {
    Good,
    Bad,
    Unknown,
}

impl Database {
    /// Create a new database
    pub fn new(name: impl Into<String>) -> Result<Self> {
        let name = name.into();

        // Validate database name
        crate::common::utils::validate_database_name(&name)?;
        
        let storage  = Arc::new(OptimisticRocksDBEngine::new(name.clone())?);
        
        
        Ok(Database {
            name,
            storage,
        })
    }
    
    /// Get database name
    pub fn name(&self) -> &str {
        &self.name
    }
    
    /// Check if this is the system database
    pub fn is_system(&self) -> bool {
        self.name == "_system"
    }
    
    /// Create a collection
    pub fn create_collection(&self, name: &str, collection_type: CollectionType) -> Result<CollectionInfo> {
        self.storage.create_collection(name, collection_type)
    }
    
    /// Drop a collection
    pub fn drop_collection(&self, name: &str) -> Result<()> {
        self.storage.drop_collection(name)
    }
    
    /// Get collection information
    pub fn get_collection(&self, name: &str) -> Option<CollectionInfo> {
        self.storage.get_collection(name)
    }
    
    /// List all collections
    pub fn list_collections(&self) -> Vec<CollectionInfo> {
        self.storage.list_collections()
    }
    
    /// Get database information
    pub fn info(&self) -> DatabaseInfo {
        DatabaseInfo {
            name: self.name.clone(),
            id: self.name.clone(),
            path: format!("databases/{}", self.name),
            is_system: self.is_system(),
        }
    }
    
    /// Get database statistics
    pub fn stats(&self) -> Result<DatabaseStats> {
        let storage_stats = self.storage.get_statistics()?;
        Ok(DatabaseStats {
            collections: storage_stats.total_collections,
            documents: storage_stats.total_documents,
            data_size: storage_stats.total_size,
        })
    }
    
    /// Get health status
    pub fn health(&self) -> HealthStatus {
        // Simple health check - in real implementation this would be more sophisticated
        HealthStatus::Good
    }
    
    /// Get storage engine reference
    pub fn storage(&self) -> &Arc<dyn StorageEngine> {
        &self.storage
    }
} 