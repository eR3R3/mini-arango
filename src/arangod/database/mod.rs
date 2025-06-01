pub mod vocbase_types;
pub mod validators;
pub mod computed_values;
pub mod logical_collection;
pub mod vocbase;

// Legacy modules (keeping for compatibility during transition)
pub mod database;
pub mod collection;

// Re-export main types from the new comprehensive implementation
pub use vocbase_types::*;
pub use validators::{
    ValidatorBase,
    ValidationLevel,
    JsonSchemaValidator,
    BooleanValidator,
    ValidatorFactory,
};
pub use computed_values::{
    ComputedValues,
    ComputedValue,
    ComputeValuesOn,
    build_computed_values_instance,
};
pub use logical_collection::{
    LogicalCollection, 
    PhysicalCollection,
    CollectionVersion,
    CollectionStatus,
    UserInputCollectionProperties,
    KeyGenerator,
    CollectionFigures,
    AliveDocumentStats,
    DeadDocumentStats,
    IndexStats,
    CacheStats,
};
pub use vocbase::{
    VocBase,
    VocBaseInfo,
    VocBaseStats,
    VocBaseManager,
    HealthStatus,
};

// Legacy re-exports (for backward compatibility)
pub use database::{Database, DatabaseInfo, DatabaseStats};
pub use collection::{Collection, CollectionInfo}; 