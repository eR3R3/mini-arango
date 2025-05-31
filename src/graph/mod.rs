pub mod graph;
pub mod graph_manager;
pub mod graph_algorithms;
mod graph;
mod algorithm;
mod managers;

// Re-export main types from graph module
pub use graph::{
    ArangoGraph,
    GraphDefinition,
    GraphType,
    EdgeDefinition,
    EdgeDirection,
    Vertex,
    Edge,
    TraversalOptions,
    TraversalResult,
    Path,
    ShortestPathOptions,
    GraphStatistics,
};

// Re-export from graph_manager
pub use graph_manager::{GraphManager, GraphInfo};

// Re-export graph algorithms and caching
pub use graph_algorithms::{
    GraphAlgorithms,
    GraphCache,
    MemoryGraphCache,
};

use crate::common::error::Result;
use crate::common::document::{Document, EdgeDocument};

/// Graph operations trait
pub trait GraphOperations {
    /// Create a new graph
    fn create_graph(&self, definition: GraphDefinition) -> Result<ArangoGraph>;

    /// Get an existing graph
    fn get_graph(&self, name: &str) -> Result<Option<ArangoGraph>>;

    /// Drop a graph
    fn drop_graph(&self, name: &str) -> Result<()>;

    /// List all graphs
    fn list_graphs(&self) -> Result<Vec<GraphInfo>>;

    /// Insert a vertex into a graph
    fn insert_vertex(&self, graph_name: &str, collection: &str, document: Document) -> Result<Vertex>;

    /// Insert an edge into a graph
    fn insert_edge(&self, graph_name: &str, collection: &str, edge_document: EdgeDocument) -> Result<Edge>;

    /// Traverse a graph
    fn traverse_graph(&self, graph_name: &str, options: TraversalOptions) -> Result<TraversalResult>;

    /// Find shortest path in a graph
    fn shortest_path(&self, graph_name: &str, options: ShortestPathOptions) -> Result<Option<Path>>;

    /// Advanced graph algorithms
    fn compute_page_rank(&self, graph_name: &str, damping_factor: f64, max_iterations: usize) -> Result<std::collections::HashMap<crate::common::document::DocumentId, f64>>;

    /// Find strongly connected components
    fn find_strongly_connected_components(&self, graph_name: &str) -> Result<Vec<Vec<crate::common::document::DocumentId>>>;

    /// Detect cycles in the graph
    fn detect_cycles(&self, graph_name: &str) -> Result<Vec<Vec<crate::common::document::DocumentId>>>;
} 