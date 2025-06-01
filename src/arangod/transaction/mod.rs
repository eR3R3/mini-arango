pub mod transaction;

// Re-export main types
pub use transaction::{
    ArangoTransactionManager, 
    ArangoTransactionContext,
    TransactionMode, 
    TransactionOptions, 
    TransactionState,
    IsolationLevel,
    // Add missing aliases
    ArangoTransactionManager as TransactionManager,
    ArangoTransactionContext as SimpleTransactionMethods,
}; 