pub mod graph;
pub mod graph_manager;
pub mod graph_algorithms;

// Re-export main types for convenience
pub use graph::{
    ArangoGraph,
    Vertex,
    Edge,
    EdgeDirection,
    GraphDefinition,
    EdgeDefinition,
    GraphTraversalOptions,
    TraversalResult,
    PathSegment,
    GraphType,
    GraphStatistics,
};

pub use graph_manager::{
    GraphManager,
    GraphInfo,
};

// Re-export graph algorithms and caching
pub use graph_algorithms::{
    TraversalCache,
    MemoryTraversalCache,
};

use crate::common::error::Result;
use crate::common::document::{Document, EdgeDocument, DocumentId};

/// Trait for graph operations
pub trait GraphOperations {
    /// Create a new graph
    fn create_graph(&self, definition: GraphDefinition) -> Result<ArangoGraph>;
    
    /// Drop a graph
    fn drop_graph(&self, name: &str) -> Result<()>;
    
    /// Get a graph by name
    fn get_graph(&self, name: &str) -> Result<Option<&ArangoGraph>>;
    
    /// List all graphs
    fn list_graphs(&self) -> Vec<String>;
    
    /// Traverse a graph
    fn traverse_graph(&self, graph_name: &str, start_vertex: &str, options: GraphTraversalOptions) -> Result<TraversalResult>;
    
    /// Insert a vertex into a graph
    fn insert_vertex(&self, graph_name: &str, collection: &str, document: Document) -> Result<Vertex>;
    
    /// Insert an edge into a graph
    fn insert_edge(&self, graph_name: &str, collection: &str, edge_document: EdgeDocument) -> Result<Edge>;
    
    /// Find shortest path in a graph
    fn shortest_path(&self, graph_name: &str, options: GraphTraversalOptions) -> Result<Option<Vec<PathSegment>>>;
    
    /// Advanced graph algorithms
    fn compute_page_rank(&self, graph_name: &str, damping_factor: f64, max_iterations: usize) -> Result<std::collections::HashMap<DocumentId, f64>>;
    
    /// Find strongly connected components
    fn find_strongly_connected_components(&self, graph_name: &str) -> Result<Vec<Vec<DocumentId>>>;
    
    /// Detect cycles in the graph
    fn detect_cycles(&self, graph_name: &str) -> Result<Vec<Vec<DocumentId>>>;
} 