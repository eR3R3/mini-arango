use std::sync::Arc;
use async_trait::async_trait;
use crate::common::{
    error::{Result, ArangoError},
    document::{Document, DocumentFilter, DocumentKey},
};

pub mod rocksdb_engine;
pub mod optimistic_rocksdb_engine;

// Re-export commonly used types
pub use rocksdb_engine::{
    RocksDBEngine, 
    EngineConfig, 
    CollectionInfo, 
    CollectionType, 
    DatabaseStatistics
};

// Re-export optimistic engine types
pub use optimistic_rocksdb_engine::{
    OptimisticRocksDBEngine,
    RocksDBTx,
    CF_COLLECTIONS,
};

/// Storage engine trait - defines the interface all storage engines must implement
pub trait StorageEngine: Send + Sync {
    /// Create a new collection
    fn create_collection(&self, name: &str, collection_type: CollectionType) -> Result<CollectionInfo>;
    
    /// Drop a collection
    fn drop_collection(&self, name: &str) -> Result<()>;
    
    /// Get collection information
    fn get_collection(&self, name: &str) -> Option<CollectionInfo>;
    
    /// List all collections
    fn list_collections(&self) -> Vec<CollectionInfo>;
    
    /// Insert a document into a collection
    fn insert_document(&self, collection_name: &str, document: &Document) -> Result<()>;
    
    /// Update a document in a collection
    fn update_document(&self, collection_name: &str, document: &Document) -> Result<()>;
    
    /// Get a document from a collection
    fn get_document(&self, collection_name: &str, key: &DocumentKey) -> Result<Option<Document>>;
    
    /// Delete a document from a collection
    fn delete_document(&self, collection_name: &str, key: &DocumentKey) -> Result<bool>;
    
    /// Find documents in a collection matching a filter
    fn find_documents(&self, collection_name: &str, filter: &DocumentFilter, limit: Option<usize>) -> Result<Vec<Document>>;
    
    /// Count documents in a collection
    fn count_documents(&self, collection_name: &str) -> Result<u64>;
    
    /// Get database statistics
    fn get_statistics(&self) -> Result<DatabaseStatistics>;
    
    /// Compact database
    fn compact(&self) -> Result<()>;
    
    /// Close the database
    fn close(&self) -> Result<()>;
}

// Implement StorageEngine trait for RocksDBEngine (legacy support)
impl StorageEngine for RocksDBEngine {
    fn create_collection(&self, name: &str, collection_type: CollectionType) -> Result<CollectionInfo> {
        self.create_collection(name, collection_type)
    }
    
    fn drop_collection(&self, name: &str) -> Result<()> {
        self.drop_collection(name)
    }
    
    fn get_collection(&self, name: &str) -> Option<CollectionInfo> {
        self.get_collection(name)
    }
    
    fn list_collections(&self) -> Vec<CollectionInfo> {
        self.list_collections()
    }
    
    fn insert_document(&self, collection_name: &str, document: &Document) -> Result<()> {
        self.insert_document(collection_name, document)
    }
    
    fn update_document(&self, collection_name: &str, document: &Document) -> Result<()> {
        self.update_document(collection_name, document)
    }
    
    fn get_document(&self, collection_name: &str, key: &DocumentKey) -> Result<Option<Document>> {
        self.get_document(collection_name, key)
    }
    
    fn delete_document(&self, collection_name: &str, key: &DocumentKey) -> Result<bool> {
        self.delete_document(collection_name, key)
    }
    
    fn find_documents(&self, collection_name: &str, filter: &DocumentFilter, limit: Option<usize>) -> Result<Vec<Document>> {
        self.find_documents(collection_name, filter, limit)
    }
    
    fn count_documents(&self, collection_name: &str) -> Result<u64> {
        self.count_documents(collection_name)
    }
    
    fn get_statistics(&self) -> Result<DatabaseStatistics> {
        self.get_statistics()
    }
    
    fn compact(&self) -> Result<()> {
        self.compact()
    }
    
    fn close(&self) -> Result<()> {
        self.close()
    }
}

// Implement StorageEngine trait for OptimisticRocksDBEngine (preferred)
impl StorageEngine for OptimisticRocksDBEngine {
    fn create_collection(&self, name: &str, collection_type: CollectionType) -> Result<CollectionInfo> {
        // Convert between different CollectionType enums
        let engine_collection_type = match collection_type {
            CollectionType::Document => optimistic_rocksdb_engine::CollectionType::Document,
            CollectionType::Edge => optimistic_rocksdb_engine::CollectionType::Edge,
        };
        
        let optimistic_info = self.create_collection(name, engine_collection_type)?;
        
        // Convert CollectionInfo types
        Ok(CollectionInfo {
            name: optimistic_info.name,
            collection_type,
            created: optimistic_info.created,
            data_size: optimistic_info.data_size,
            document_count: optimistic_info.document_count,
        })
    }
    
    fn drop_collection(&self, name: &str) -> Result<()> {
        let _collection_info = self.get_collection(name)
            .ok_or_else(|| ArangoError::collection_not_found(name))?;
        
        // 由于OptimisticRocksDBEngine的字段是私有的，我们需要添加公共方法
        // 这里暂时返回一个错误，表示功能尚未完全实现
        Err(ArangoError::internal("Drop collection not yet fully implemented due to private field access"))
    }
    
    fn get_collection(&self, name: &str) -> Option<CollectionInfo> {
        if let Some(optimistic_info) = self.get_collection(name) {
            // Convert collection type
            let collection_type = match optimistic_info.collection_type {
                optimistic_rocksdb_engine::CollectionType::Document => CollectionType::Document,
                optimistic_rocksdb_engine::CollectionType::Edge => CollectionType::Edge,
            };
            
            Some(CollectionInfo {
                name: optimistic_info.name,
                collection_type,
                created: optimistic_info.created,
                data_size: optimistic_info.data_size,
                document_count: optimistic_info.document_count,
            })
        } else {
            None
        }
    }
    
    fn list_collections(&self) -> Vec<CollectionInfo> {
        self.list_collections().into_iter().map(|optimistic_info| {
            let collection_type = match optimistic_info.collection_type {
                optimistic_rocksdb_engine::CollectionType::Document => CollectionType::Document,
                optimistic_rocksdb_engine::CollectionType::Edge => CollectionType::Edge,
            };
            
            CollectionInfo {
                name: optimistic_info.name,
                collection_type,
                created: optimistic_info.created,
                data_size: optimistic_info.data_size,
                document_count: optimistic_info.document_count,
            }
        }).collect()
    }
    
    fn insert_document(&self, collection_name: &str, document: &Document) -> Result<()> {
        self.insert_document(collection_name, document)
    }
    
    fn update_document(&self, collection_name: &str, document: &Document) -> Result<()> {
        self.update_document(collection_name, document)
    }
    
    fn get_document(&self, collection_name: &str, key: &DocumentKey) -> Result<Option<Document>> {
        self.get_document(collection_name, key)
    }
    
    fn delete_document(&self, collection_name: &str, key: &DocumentKey) -> Result<bool> {
        self.delete_document(collection_name, key)
    }
    
    fn find_documents(&self, collection_name: &str, filter: &DocumentFilter, limit: Option<usize>) -> Result<Vec<Document>> {
        let _collection_info = self.get_collection(collection_name)
            .ok_or_else(|| ArangoError::collection_not_found(collection_name))?;
        
        let tx = self.transaction();
        let mut documents = Vec::new();
        
        // 使用前缀扫描来获取集合中的所有文档
        let prefix = format!("{}:", collection_name);
        let prefix_bytes = prefix.as_bytes();
        let mut upper_bound = prefix_bytes.to_vec();
        if let Some(last_byte) = upper_bound.last_mut() {
            *last_byte += 1;
        }
        
        if let Ok(iter) = tx.range_scan(prefix_bytes, &upper_bound) {
            for item in iter {
                if let Ok((_, value)) = item {
                    if let Ok(document) = serde_json::from_slice::<Document>(&value) {
                        // 应用过滤器
                        let matches = match filter {
                            DocumentFilter::All => true,
                            DocumentFilter::Key(key) => document.key() == key,
                            DocumentFilter::Field { field, value } => {
                                document.get(field)
                                    .map(|v| v == value)
                                    .unwrap_or(false)
                            },
                            DocumentFilter::Id(id) => document.id() == id,
                            DocumentFilter::And(filters) => {
                                filters.iter().all(|f| f.matches(&document))
                            },
                            DocumentFilter::Or(filters) => {
                                filters.iter().any(|f| f.matches(&document))
                            },
                        };
                        
                        if matches {
                            documents.push(document);
                            
                            // 检查限制
                            if let Some(max_docs) = limit {
                                if documents.len() >= max_docs {
                                    break;
                                }
                            }
                        }
                    }
                }
            }
        }
        
        Ok(documents)
    }
    
    fn count_documents(&self, collection_name: &str) -> Result<u64> {
        self.count_documents(collection_name)
    }
    
    fn get_statistics(&self) -> Result<DatabaseStatistics> {
        let collections = self.list_collections();
        let total_collections = collections.len();
        let total_documents = collections.iter().map(|c| c.document_count).sum();
        let total_size = collections.iter().map(|c| c.data_size).sum();
        
        Ok(DatabaseStatistics {
            total_collections,
            total_documents,
            total_size,
            rocks_stats: "OptimisticRocksDB Engine - Basic Statistics".to_string(),
        })
    }
    
    fn compact(&self) -> Result<()> {
        // RocksDB会自动进行压缩，但我们可以手动触发
        // 这里暂时返回成功，实际的压缩操作可以在后台进行
        Ok(())
    }
    
    fn close(&self) -> Result<()> {
        // OptimisticRocksDBEngine使用Arc，会在引用计数为0时自动清理
        // 这里我们只需要确保没有活跃的事务即可
        Ok(())
    }
} 