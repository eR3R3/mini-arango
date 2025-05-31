mod optimistic;
mod raw;

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
        self.create_collection(name, collection_type)
    }

    fn drop_collection(&self, name: &str) -> Result<()> {
        // TODO: Implement drop_collection for OptimisticRocksDBEngine
        todo!("Drop collection not yet implemented for OptimisticRocksDBEngine")
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
        // TODO: Implement find_documents for OptimisticRocksDBEngine
        todo!("Find documents not yet implemented for OptimisticRocksDBEngine")
    }

    fn count_documents(&self, collection_name: &str) -> Result<u64> {
        // TODO: Implement count_documents for OptimisticRocksDBEngine
        todo!("Count documents not yet implemented for OptimisticRocksDBEngine")
    }

    fn get_statistics(&self) -> Result<DatabaseStatistics> {
        // TODO: Implement get_statistics for OptimisticRocksDBEngine
        todo!("Get statistics not yet implemented for OptimisticRocksDBEngine")
    }

    fn compact(&self) -> Result<()> {
        // TODO: Implement compact for OptimisticRocksDBEngine
        todo!("Compact not yet implemented for OptimisticRocksDBEngine")
    }

    fn close(&self) -> Result<()> {
        // TODO: Implement close for OptimisticRocksDBEngine
        todo!("Close not yet implemented for OptimisticRocksDBEngine")
    }
} 