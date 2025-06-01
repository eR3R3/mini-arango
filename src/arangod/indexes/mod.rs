pub mod indexes;
pub mod geo_index;
pub mod index_factory;

// Re-export main types
pub use indexes::{
    IndexManager, 
    IndexDefinition, 
    IndexType, 
    RocksDBIndex, 
    IndexStatistics,
    FilterCosts,
    SortCosts,
};

// Re-export geo index types
pub use geo_index::{
    GeoIndex,
    GeoPoint,
    BoundingBox,
};

// Re-export specialized index implementations
pub use index_factory::{
    IndexImplementation,
    IndexFactory,
    BasicIndex,
    VectorIndex,
    FulltextIndex,
    SimilarityMetric,
}; 