// Graph - Core graph representation for document-graph database
// Similar to ArangoDB's Graph.cpp

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use serde::{Deserialize, Serialize};
use petgraph::{Graph, Directed, Undirected};
use petgraph::graph::{NodeIndex, EdgeIndex};
use petgraph::algo::{dijkstra, astar};
use petgraph::visit::EdgeRef;
use crate::common::error::{ArangoError, Result, ErrorCode};
use crate::common::document::{Document, DocumentId, DocumentKey, EdgeDocument};
use crate::arangod::storage_engine::{StorageEngine, CollectionType};
use ordered_float::OrderedFloat;

/// A segment of a graph path
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PathSegment {
    pub vertex: Vertex,
    pub edge: Option<Edge>,
}

impl PathSegment {
    pub fn new(vertex: Vertex, edge: Option<Edge>) -> Self {
        PathSegment { vertex, edge }
    }
}

/// Graph type enumeration
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum GraphType {
    /// Directed graph
    Directed,
    /// Standard graph
    Standard,
    /// Smart graph (for enterprise)
    Smart,
    /// Satellite graph (for enterprise) 
    Satellite,
}

impl Default for GraphType {
    fn default() -> Self {
        GraphType::Directed
    }
}

/// Edge direction for traversal
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum EdgeDirection {
    /// Outbound edges only
    Outbound,
    /// Inbound edges only
    Inbound,
    /// Both directions
    Any,
}

/// Graph definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GraphDefinition {
    /// Graph name
    pub name: String,
    /// Edge definitions
    pub edge_definitions: Vec<EdgeDefinition>,
    /// Orphan collections (vertex collections not used in edge definitions)
    pub orphan_collections: Vec<String>,
    /// Graph type
    pub graph_type: GraphType,
    /// Whether this is a smart graph (enterprise feature)
    pub is_smart: bool,
}

/// Edge definition in a graph
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EdgeDefinition {
    /// Edge collection name
    pub collection: String,
    /// Source vertex collections
    pub from: Vec<String>,
    /// Target vertex collections  
    pub to: Vec<String>,
}

/// Vertex representation
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Vertex {
    /// Vertex document
    pub document: Document,
}

impl Vertex {
    /// Create a new vertex
    pub fn new(document: Document) -> Self {
        Vertex { document }
    }
    
    /// Get vertex ID
    pub fn id(&self) -> &DocumentId {
        self.document.id()
    }
    
    /// Get vertex key
    pub fn key(&self) -> &DocumentKey {
        self.document.key()
    }
    
    /// Get vertex collection
    pub fn collection(&self) -> &str {
        self.document.collection()
    }
}

/// Edge representation
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Edge {
    /// Edge document
    pub document: EdgeDocument,
}

impl Edge {
    /// Create a new edge
    pub fn new(document: EdgeDocument) -> Self {
        Edge { document }
    }
    
    /// Get edge ID
    pub fn id(&self) -> &DocumentId {
        self.document.document().id()
    }
    
    /// Get edge key
    pub fn key(&self) -> &DocumentKey {
        self.document.document().key()
    }
    
    /// Get edge collection
    pub fn collection(&self) -> &str {
        self.document.document().collection()
    }
    
    /// Get source vertex ID
    pub fn from(&self) -> &DocumentId {
        self.document.from()
    }
    
    /// Get target vertex ID
    pub fn to(&self) -> &DocumentId {
        self.document.to()
    }
    
    /// Get edge weight (if specified in document data)
    pub fn weight(&self) -> f64 {
        self.document.document()
            .get("weight")
            .and_then(|v| v.as_f64())
            .unwrap_or(1.0)
    }
}

/// Graph traversal options for configuring graph algorithms
pub struct GraphTraversalOptions {
    pub max_depth: usize,
    pub min_depth: usize,
    pub direction: EdgeDirection,
    pub vertex_filter: Option<Box<dyn Fn(&Vertex) -> bool + Send + Sync>>,
    pub edge_filter: Option<Box<dyn Fn(&Edge) -> bool + Send + Sync>>,
    pub unique_vertices: bool,
    pub unique_edges: bool,
}

impl GraphTraversalOptions {
    pub fn new() -> Self {
        GraphTraversalOptions {
            max_depth: 10,
            min_depth: 1,
            direction: EdgeDirection::Outbound,
            vertex_filter: None,
            edge_filter: None,
            unique_vertices: true,
            unique_edges: true,
        }
    }
}

impl Default for GraphTraversalOptions {
    fn default() -> Self {
        Self::new()
    }
}

/// Traversal result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TraversalResult {
    /// Vertices found
    pub vertices: Vec<Vertex>,
    /// Edges traversed
    pub edges: Vec<Edge>,
    /// Paths (sequences of vertices and edges)
    pub paths: Vec<Path>,
}

/// Path in a graph traversal
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct Path {
    pub vertices: Vec<Vertex>,
    pub edges: Vec<Edge>,
    pub weight: OrderedFloat<f64>,
}

impl Path {
    pub fn new() -> Self {
        Path {
            vertices: Vec::new(),
            edges: Vec::new(),
            weight: OrderedFloat(0.0),
        }
    }
    
    pub fn add_vertex(&mut self, vertex: Vertex) {
        self.vertices.push(vertex);
    }
    
    pub fn add_edge(&mut self, edge: Edge) {
        self.edges.push(edge);
    }
    
    pub fn length(&self) -> usize {
        self.vertices.len()
    }
    
    pub fn weight_f64(&self) -> f64 {
        self.weight.into_inner()
    }
    
    pub fn set_weight(&mut self, weight: f64) {
        self.weight = OrderedFloat(weight);
    }
}

impl Default for Path {
    fn default() -> Self {
        Self::new()
    }
}

/// Shortest path options
#[derive(Debug, Clone)]
pub struct ShortestPathOptions {
    /// Starting vertex
    pub start_vertex: DocumentId,
    /// Target vertex
    pub target_vertex: DocumentId,
    /// Edge direction
    pub direction: EdgeDirection,
    /// Weight attribute name
    pub weight_attribute: Option<String>,
    /// Default weight if attribute not found
    pub default_weight: f64,
}

impl Default for ShortestPathOptions {
    fn default() -> Self {
        ShortestPathOptions {
            start_vertex: DocumentId::parse("dummy/dummy").unwrap(),
            target_vertex: DocumentId::parse("dummy/dummy").unwrap(),
            direction: EdgeDirection::Outbound,
            weight_attribute: Some("weight".to_string()),
            default_weight: 1.0,
        }
    }
}

/// Graph implementation
pub struct ArangoGraph {
    /// Graph definition
    definition: GraphDefinition,
    /// Storage engine reference
    storage: Arc<dyn StorageEngine>,
    /// Cached vertex index mapping (document_id -> node_index)
    vertex_index_cache: HashMap<DocumentId, NodeIndex>,
    /// Cached edge index mapping (document_id -> edge_index)
    edge_index_cache: HashMap<DocumentId, EdgeIndex>,
    /// Petgraph representation (for algorithms)
    petgraph: Graph<DocumentId, DocumentId, Directed>,
}

impl ArangoGraph {
    /// Create a new graph
    pub fn new(definition: GraphDefinition, storage: Arc<dyn StorageEngine>) -> Result<Self> {
        // Validate edge definitions
        for edge_def in &definition.edge_definitions {
            // Check if edge collection exists and is of type Edge
            if let Some(collection_info) = storage.get_collection(&edge_def.collection) {
                if collection_info.collection_type != CollectionType::Edge {
                    return Err(ArangoError::new(
                        ErrorCode::GraphWrongCollectionTypeVertex,
                        format!("Collection '{}' is not an edge collection", edge_def.collection)
                    ));
                }
            } else {
                return Err(ArangoError::new(
                    ErrorCode::GraphEdgeColDoesNotExist,
                    format!("Edge collection '{}' does not exist", edge_def.collection)
                ));
            }
            
            // Check vertex collections
            for vertex_collection in edge_def.from.iter().chain(edge_def.to.iter()) {
                if let Some(collection_info) = storage.get_collection(vertex_collection) {
                    if collection_info.collection_type != CollectionType::Document {
                        return Err(ArangoError::new(
                            ErrorCode::GraphWrongCollectionTypeVertex,
                            format!("Collection '{}' is not a document collection", vertex_collection)
                        ));
                    }
                }
            }
        }
        
        let mut graph = ArangoGraph {
            definition,
            storage,
            vertex_index_cache: HashMap::new(),
            edge_index_cache: HashMap::new(),
            petgraph: Graph::new(),
        };
        
        // Build the graph structure
        graph.rebuild_graph_structure()?;
        
        Ok(graph)
    }
    
    /// Rebuild the internal graph structure from storage
    fn rebuild_graph_structure(&mut self) -> Result<()> {
        self.petgraph.clear();
        self.vertex_index_cache.clear();
        self.edge_index_cache.clear();
        
        // Clone the definition to avoid borrowing issues
        let edge_definitions = self.definition.edge_definitions.clone();
        let orphan_collections = self.definition.orphan_collections.clone();
        
        // Add all vertices first
        for edge_def in &edge_definitions {
            for vertex_collection in edge_def.from.iter().chain(edge_def.to.iter()) {
                self.load_vertices_from_collection(vertex_collection)?;
            }
        }
        
        // Add orphan collections
        for orphan_collection in &orphan_collections {
            self.load_vertices_from_collection(orphan_collection)?;
        }
        
        // Add all edges
        for edge_def in &edge_definitions {
            self.load_edges_from_collection(&edge_def.collection)?;
        }
        
        Ok(())
    }
    
    /// Load vertices from a collection
    fn load_vertices_from_collection(&mut self, collection_name: &str) -> Result<()> {
        use crate::common::document::DocumentFilter;
        
        let documents = self.storage.find_documents(collection_name, &DocumentFilter::All, None)?;
        
        for document in documents {
            let document_id = document.id().clone();
            if !self.vertex_index_cache.contains_key(&document_id) {
                let node_index = self.petgraph.add_node(document_id.clone());
                self.vertex_index_cache.insert(document_id, node_index);
            }
        }
        
        Ok(())
    }
    
    /// Load edges from a collection
    fn load_edges_from_collection(&mut self, collection_name: &str) -> Result<()> {
        use crate::common::document::DocumentFilter;
        
        let documents = self.storage.find_documents(collection_name, &DocumentFilter::All, None)?;
        
        for document in documents {
            // Parse edge document to extract _from and _to
            if let (Some(from_value), Some(to_value)) = (document.get("_from"), document.get("_to")) {
                if let (Some(from_str), Some(to_str)) = (from_value.as_str(), to_value.as_str()) {
                    let from_id = DocumentId::parse(from_str)?;
                    let to_id = DocumentId::parse(to_str)?;
                    
                    // Get or create vertex indices
                    let from_index = self.get_or_create_vertex_index(from_id.clone());
                    let to_index = self.get_or_create_vertex_index(to_id.clone());
                    
                    // Add edge to petgraph
                    let edge_index = self.petgraph.add_edge(from_index, to_index, document.id().clone());
                    self.edge_index_cache.insert(document.id().clone(), edge_index);
                }
            }
        }
        
        Ok(())
    }
    
    /// Get or create vertex index in petgraph
    fn get_or_create_vertex_index(&mut self, document_id: DocumentId) -> NodeIndex {
        if let Some(&index) = self.vertex_index_cache.get(&document_id) {
            index
        } else {
            let index = self.petgraph.add_node(document_id.clone());
            self.vertex_index_cache.insert(document_id, index);
            index
        }
    }
    
    /// Traverse the graph
    pub fn traverse(&self, start_vertex: &DocumentId, options: &GraphTraversalOptions) -> Result<TraversalResult> {
        if let Some(start_index) = self.vertex_index_cache.get(start_vertex) {
            // Use the start_index for traversal
            let mut result = TraversalResult {
                vertices: Vec::new(),
                edges: Vec::new(),
                paths: Vec::new(),
            };
            
            // Add start vertex
            if let Ok(Some(start_doc)) = self.storage.get_document(
                start_vertex.collection(), 
                start_vertex.key()
            ) {
                result.vertices.push(Vertex {
                    document: start_doc,
                });
            }
            
            Ok(result)
        } else {
            Err(ArangoError::not_found(format!("Vertex '{}' not found in graph", start_vertex)))
        }
    }
    
    /// Find shortest path between two vertices
    pub fn shortest_path(&self, options: &GraphTraversalOptions) -> Result<Option<Vec<PathSegment>>> {
        // Simplified shortest path implementation
        // In a real implementation, this would use Dijkstra's or A* algorithm
        Ok(None)
    }
    
    /// Get graph definition
    pub fn definition(&self) -> &GraphDefinition {
        &self.definition
    }
    
    /// Get graph statistics
    pub fn statistics(&self) -> GraphStatistics {
        GraphStatistics {
            vertex_count: self.vertex_index_cache.len() as u64,
            edge_count: self.edge_index_cache.len() as u64,
            collection_count: (self.definition.edge_definitions.len() + self.definition.orphan_collections.len()) as u64,
            memory_usage: 0, // Calculate based on actual usage
        }
    }
    
    /// Get all vertices in the graph
    pub fn get_all_vertices(&self) -> Result<Vec<Vertex>> {
        let mut vertices = Vec::new();
        
        // Collect vertices from all vertex collections
        for vertex_collection in &self.definition.orphan_collections {
            // In a real implementation, this would query the actual collection
            // For now, return empty result
        }
        
        // Collect vertices from edge definitions
        for edge_def in &self.definition.edge_definitions {
            for collection in edge_def.from.iter().chain(edge_def.to.iter()) {
                // In a real implementation, this would query the actual collection
                // For now, return empty result
            }
        }
        
        Ok(vertices)
    }
    
    /// Get vertex document by ID
    pub fn get_vertex_document(&self, vertex_id: &DocumentId) -> Result<Option<Document>> {
        // In a real implementation, this would query the storage engine
        // For now, return None
        Ok(None)
    }
    
    /// Get edge between two vertices
    pub fn get_edge_between(&self, from: &DocumentId, to: &DocumentId) -> Result<Option<Document>> {
        // In a real implementation, this would query the edge collections
        // For now, return None
        Ok(None)
    }
    
    /// Get edges from a vertex
    pub fn get_edges_from(&self, vertex_id: &DocumentId) -> Result<Vec<Edge>> {
        // In a real implementation, this would query the edge collections
        // For now, return empty
        Ok(Vec::new())
    }
    
    /// Get edges to a vertex
    pub fn get_edges_to(&self, vertex_id: &DocumentId) -> Result<Vec<Edge>> {
        // In a real implementation, this would query the edge collections
        // For now, return empty
        Ok(Vec::new())
    }
    
    /// Clone the structure of the graph (for algorithms)
    pub fn clone_structure(&self) -> ArangoGraph {
        ArangoGraph {
            definition: self.definition.clone(),
            storage: self.storage.clone(),
            vertex_index_cache: self.vertex_index_cache.clone(),
            edge_index_cache: self.edge_index_cache.clone(),
            petgraph: self.petgraph.clone(),
        }
    }
}

impl Clone for ArangoGraph {
    fn clone(&self) -> Self {
        self.clone_structure()
    }
}

/// Graph statistics
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct GraphStatistics {
    pub vertex_count: u64,
    pub edge_count: u64,
    pub collection_count: u64,
    pub memory_usage: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use crate::arangod::storage_engine::RocksDBEngine;
    use tempfile::TempDir;
    use serde_json::json;

    fn create_test_setup() -> (Arc<RocksDBEngine>, TempDir) {
        let temp_dir = TempDir::new().unwrap();
        let storage = Arc::new(RocksDBEngine::new(temp_dir.path()).unwrap());
        (storage, temp_dir)
    }

    #[test]
    fn test_graph_creation() {
        let (storage, _temp_dir) = create_test_setup();
        
        // Create collections
        storage.create_collection("vertices", CollectionType::Document).unwrap();
        storage.create_collection("edges", CollectionType::Edge).unwrap();
        
        let definition = GraphDefinition {
            name: "test_graph".to_string(),
            edge_definitions: vec![EdgeDefinition {
                collection: "edges".to_string(),
                from: vec!["vertices".to_string()],
                to: vec!["vertices".to_string()],
            }],
            orphan_collections: vec![],
            graph_type: GraphType::Directed,
            is_smart: false,
        };
        
        let graph = ArangoGraph::new(definition, storage).unwrap();
        assert_eq!(graph.definition().name, "test_graph");
    }
} 