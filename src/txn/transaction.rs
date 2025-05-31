use std::sync::Arc;
use std::time::{Duration, Instant};
use serde::{Deserialize, Serialize};
use crate::common::error::{ArangoError, Result};
use crate::arangod::storage_engine::optimistic_rocksdb_engine::{OptimisticRocksDBEngine, RocksDBTx};
use crate::common::document::{Document, DocumentKey};

/// Transaction isolation levels
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum IsolationLevel {
    ReadCommitted,
    RepeatableRead,
    Serializable,
}

impl Default for IsolationLevel {
    fn default() -> Self {
        IsolationLevel::ReadCommitted
    }
}

/// Transaction mode
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TransactionMode {
    ReadOnly,
    ReadWrite,
}

impl Default for TransactionMode {
    fn default() -> Self {
        TransactionMode::ReadWrite
    }
}

/// Transaction state
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransactionState {
    Active,
    Committed,
    Aborted,
}

/// Enhanced transaction options for OptimisticTransactionDB
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransactionOptions {
    pub mode: TransactionMode,
    pub isolation_level: IsolationLevel,
    pub wait_for_sync: bool,
    pub timeout: Duration,
    pub max_retries: u32,
    pub lock_timeout: Option<Duration>,
    pub max_transaction_size: Option<u64>,
}

impl Default for TransactionOptions {
    fn default() -> Self {
        TransactionOptions {
            mode: TransactionMode::ReadWrite,
            isolation_level: IsolationLevel::ReadCommitted,
            wait_for_sync: false,
            timeout: Duration::from_secs(30),
            max_retries: 3,
            lock_timeout: Some(Duration::from_secs(5)),
            max_transaction_size: Some(100 * 1024 * 1024), // 100MB
        }
    }
}

/// Transaction manager for OptimisticRocksDBEngine
pub struct ArangoTransactionManager {
    engine: Arc<OptimisticRocksDBEngine>,
}

impl ArangoTransactionManager {
    /// Create a new transaction manager
    pub fn new(engine: Arc<OptimisticRocksDBEngine>) -> Self {
        ArangoTransactionManager { engine }
    }

    /// Begin a new transaction
    pub fn begin_transaction(&self, options: TransactionOptions) -> Result<ArangoTransactionContext> {
        Ok(ArangoTransactionContext::new(self.engine.as_ref(), options))
    }

    /// Execute a function within a transaction with automatic retry
    pub fn with_transaction<T, F>(&self, options: TransactionOptions, mut func: F) -> Result<T>
    where
        F: FnMut(&mut ArangoTransactionContext) -> Result<T>,
    {
        let mut retry_count = 0;

        loop {
            let mut txn_context = self.begin_transaction(options.clone())?;

            // Check timeout
            if txn_context.is_timeout() {
                return Err(ArangoError::internal("Transaction timeout before start"));
            }

            match func(&mut txn_context) {
                Ok(result) => {
                    match txn_context.commit() {
                        Ok(_) => return Ok(result),
                        Err(ArangoError::TransactionConflict(_)) if retry_count < options.max_retries => {
                            retry_count += 1;
                            println!("Transaction conflict, retrying ({}/{})", retry_count, options.max_retries);

                            // Exponential backoff
                            let delay = Duration::from_millis(10 * (2_u64.pow(retry_count as u32)));
                            std::thread::sleep(delay);
                            continue;
                        },
                        Err(e) => return Err(e),
                    }
                },
                Err(e) => {
                    let _ = txn_context.rollback();

                    // Check if it's a retriable error
                    if matches!(e, ArangoError::TransactionConflict(_)) && retry_count < options.max_retries {
                        retry_count += 1;
                        println!("Transaction error, retrying ({}/{}): {}", retry_count, options.max_retries, e);

                        let delay = Duration::from_millis(10 * (2_u64.pow(retry_count as u32)));
                        std::thread::sleep(delay);
                        continue;
                    }

                    return Err(e);
                }
            }
        }
    }

    /// Get the underlying engine reference
    pub fn engine(&self) -> &Arc<OptimisticRocksDBEngine> {
        &self.engine
    }
}

/// Transaction context that wraps OptimisticRocksDBEngine transactions
pub struct ArangoTransactionContext<'a> {
    /// Active transaction (None after commit/rollback)
    tx: Option<RocksDBTx<'a>>,
    options: TransactionOptions,
    state: TransactionState,
    start_time: Instant,
    retry_count: u32,
}

impl<'a> ArangoTransactionContext<'a> {
    /// Create a new transaction context
    fn new(engine: &'a OptimisticRocksDBEngine, options: TransactionOptions) -> Self {
        let tx = engine.transaction();
        ArangoTransactionContext {
            tx: Some(tx),
            options,
            state: TransactionState::Active,
            start_time: Instant::now(),
            retry_count: 0,
        }
    }

    /// Get the current transaction state
    pub fn state(&self) -> TransactionState {
        self.state
    }

    /// Check if the transaction is active
    pub fn is_active(&self) -> bool {
        matches!(self.state, TransactionState::Active) && self.tx.is_some()
    }

    /// Check if the transaction has timed out
    pub fn is_timeout(&self) -> bool {
        self.start_time.elapsed() > self.options.timeout
    }

    /// Get the retry count
    pub fn retry_count(&self) -> u32 {
        self.retry_count
    }

    /// Get transaction options
    pub fn options(&self) -> &TransactionOptions {
        &self.options
    }

    /// Execute operations within the current transaction
    pub fn execute<T, F>(&mut self, mut operation: F) -> Result<T>
    where
        F: FnMut(&mut RocksDBTx) -> Result<T>,
    {
        if !self.is_active() {
            return Err(ArangoError::internal("Transaction is not active"));
        }

        if self.is_timeout() {
            return Err(ArangoError::internal("Transaction timeout"));
        }

        let tx = self.tx.as_mut()
            .ok_or_else(|| ArangoError::internal("Transaction already committed"))?;

        operation(tx)
    }

    /// Put a key-value pair within the current transaction
    pub fn put(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        self.execute(|tx| tx.put(key, value))
    }

    /// Get a value by key within the current transaction
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        if !self.is_active() {
            return Err(ArangoError::internal("Transaction is not active"));
        }

        if self.is_timeout() {
            return Err(ArangoError::internal("Transaction timeout"));
        }

        let tx = self.tx.as_ref()
            .ok_or_else(|| ArangoError::internal("Transaction already committed"))?;

        tx.get(key)
    }

    /// Delete a key within the current transaction
    pub fn delete(&mut self, key: &[u8]) -> Result<()> {
        self.execute(|tx| tx.delete(key))
    }

    /// Check if a key exists within the current transaction
    pub fn exists(&self, key: &[u8]) -> Result<bool> {
        if !self.is_active() {
            return Err(ArangoError::internal("Transaction is not active"));
        }

        if self.is_timeout() {
            return Err(ArangoError::internal("Transaction timeout"));
        }

        let tx = self.tx.as_ref()
            .ok_or_else(|| ArangoError::internal("Transaction already committed"))?;

        tx.exists(key)
    }

    /// Range scan within the current transaction
    pub fn range_scan(&self, lower: &[u8], upper: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        if !self.is_active() {
            return Err(ArangoError::internal("Transaction is not active"));
        }

        if self.is_timeout() {
            return Err(ArangoError::internal("Transaction timeout"));
        }

        let tx = self.tx.as_ref()
            .ok_or_else(|| ArangoError::internal("Transaction already committed"))?;

        let iter = tx.range_scan(lower, upper)?;
        iter.collect()
    }

    /// Count items in range within the current transaction
    pub fn range_count(&self, lower: &[u8], upper: &[u8]) -> Result<usize> {
        if !self.is_active() {
            return Err(ArangoError::internal("Transaction is not active"));
        }

        if self.is_timeout() {
            return Err(ArangoError::internal("Transaction timeout"));
        }

        let tx = self.tx.as_ref()
            .ok_or_else(|| ArangoError::internal("Transaction already committed"))?;

        tx.range_count(lower, upper)
    }

    /// Insert a document within the current transaction
    pub fn insert_document(&mut self, collection_name: &str, document: &Document) -> Result<()> {
        self.execute(|tx| tx.insert_document(collection_name, document))
    }

    /// Update a document within the current transaction
    pub fn update_document(&mut self, collection_name: &str, document: &Document) -> Result<()> {
        self.execute(|tx| tx.update_document(collection_name, document))
    }

    /// Delete a document within the current transaction
    pub fn delete_document(&mut self, collection_name: &str, key: &DocumentKey) -> Result<bool> {
        self.execute(|tx| tx.delete_document(collection_name, key))
    }

    /// Get a document within the current transaction
    pub fn get_document(&self, collection_name: &str, key: &DocumentKey) -> Result<Option<Document>> {
        if !self.is_active() {
            return Err(ArangoError::internal("Transaction is not active"));
        }

        if self.is_timeout() {
            return Err(ArangoError::internal("Transaction timeout"));
        }

        let tx = self.tx.as_ref()
            .ok_or_else(|| ArangoError::internal("Transaction already committed"))?;

        tx.get_document(collection_name, key)
    }

    /// Execute multiple operations in the current transaction (batch operations)
    pub fn batch_execute<T, F>(&mut self, operations: F) -> Result<T>
    where
        F: FnMut(&mut RocksDBTx) -> Result<T>,
    {
        self.execute(operations)
    }

    /// Mark transaction as committed (for external commit)
    pub fn mark_committed(&mut self) {
        self.state = TransactionState::Committed;
    }

    /// Mark transaction as aborted (for external rollback)
    pub fn mark_aborted(&mut self) {
        self.state = TransactionState::Aborted;
    }

    /// Commit the transaction
    pub fn commit(&mut self) -> Result<()> {
        if !self.is_active() {
            return Err(ArangoError::internal("Transaction is not active"));
        }

        let tx = self.tx.take()
            .ok_or_else(|| ArangoError::internal("Transaction already committed"))?;

        match tx.commit() {
            Ok(_) => {
                self.state = TransactionState::Committed;
                Ok(())
            },
            Err(e) => {
                self.state = TransactionState::Aborted;
                Err(e)
            }
        }
    }

    /// Rollback the transaction
    pub fn rollback(&mut self) -> Result<()> {
        if matches!(self.state, TransactionState::Committed) {
            return Err(ArangoError::internal("Cannot rollback committed transaction"));
        }

        if let Some(tx) = self.tx.take() {
            tx.rollback()?;
        }

        self.state = TransactionState::Aborted;
        Ok(())
    }
}

// Implement Drop to ensure transaction is cleaned up
impl<'a> Drop for ArangoTransactionContext<'a> {
    fn drop(&mut self) {
        if self.is_active() {
            let _ = self.rollback();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;
    use serde_json::json;

    fn create_test_manager() -> (ArangoTransactionManager, TempDir) {
        let temp_dir = TempDir::new().unwrap();
        let engine = OptimisticRocksDBEngine::new(temp_dir.path()).unwrap();
        let manager = ArangoTransactionManager::new(Arc::new(engine));
        (manager, temp_dir)
    }

    #[test]
    fn test_transaction_manager_basic() {
        let (manager, _temp_dir) = create_test_manager();

        // Test basic transaction operations
        let result = manager.with_transaction(TransactionOptions::default(), |txn| {
            // Put some data
            txn.put(b"test_key", b"test_value")?;

            // Get it back
            let value = txn.get(b"test_key")?;
            assert_eq!(value, Some(b"test_value".to_vec()));

            Ok(())
        });

        assert!(result.is_ok());
    }

    #[test]
    fn test_transaction_timeout() {
        let (manager, _temp_dir) = create_test_manager();

        let mut options = TransactionOptions::default();
        options.timeout = Duration::from_millis(1); // Very short timeout

        let result = manager.with_transaction(options, |txn| {
            // Sleep longer than timeout
            std::thread::sleep(Duration::from_millis(10));
            txn.put(b"test", b"value")?;
            Ok(())
        });

        assert!(result.is_err());
    }

    #[test]
    fn test_transaction_rollback() {
        let (manager, _temp_dir) = create_test_manager();

        // Transaction that will fail
        let result = manager.with_transaction(TransactionOptions::default(), |txn| {
            txn.put(b"test", b"value")?;

            // Force an error
            Err(ArangoError::internal("Forced error"))
        });

        assert!(result.is_err());

        // Verify data was not persisted
        let verify_result = manager.with_transaction(TransactionOptions::default(), |txn| {
            let value = txn.get(b"test")?;
            assert!(value.is_none());
            Ok(())
        });

        assert!(verify_result.is_ok());
    }

    #[test]
    fn test_isolation_levels() {
        let (manager, _temp_dir) = create_test_manager();

        for level in [IsolationLevel::ReadCommitted, IsolationLevel::RepeatableRead, IsolationLevel::Serializable] {
            let mut options = TransactionOptions::default();
            options.isolation_level = level;

            let result = manager.with_transaction(options, |txn| {
                txn.put(format!("test_{:?}", level).as_bytes(), b"value")?;
                Ok(())
            });

            assert!(result.is_ok(), "Failed for isolation level: {:?}", level);
        }
    }
} 