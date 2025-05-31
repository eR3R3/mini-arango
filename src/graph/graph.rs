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

/// Graph type
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum GraphType {
    /// Directed graph
    Directed,
    /// Undirected graph  
    Undirected,
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
#[derive(Debug, Clone, Serialize, Deserialize)]
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
#[derive(Debug, Clone, Serialize, Deserialize)]
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

/// Graph traversal options
#[derive(Debug, Clone)]
pub struct TraversalOptions {
    /// Starting vertex
    pub start_vertex: DocumentId,
    /// Edge direction
    pub direction: EdgeDirection,
    /// Minimum depth
    pub min_depth: u32,
    /// Maximum depth
    pub max_depth: u32,
    /// Maximum number of results
    pub limit: Option<usize>,
    /// Vertex filter function
    pub vertex_filter: Option<Box<dyn Fn(&Vertex) -> bool + Send + Sync>>,
    /// Edge filter function
    pub edge_filter: Option<Box<dyn Fn(&Edge) -> bool + Send + Sync>>,
    /// Unique vertices (avoid revisiting)
    pub unique_vertices: bool,
    /// Unique edges (avoid reusing edges)
    pub unique_edges: bool,
}

impl Default for TraversalOptions {
    fn default() -> Self {
        TraversalOptions {
            start_vertex: DocumentId::parse("dummy/dummy").unwrap(),
            direction: EdgeDirection::Outbound,
            min_depth: 1,
            max_depth: 1,
            limit: None,
            vertex_filter: None,
            edge_filter: None,
            unique_vertices: true,
            unique_edges: true,
        }
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
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Path {
    /// Vertices in the path
    pub vertices: Vec<Vertex>,
    /// Edges in the path
    pub edges: Vec<Edge>,
    /// Total weight of the path
    pub weight: f64,
}

impl Path {
    /// Create a new empty path
    pub fn new() -> Self {
        Path {
            vertices: Vec::new(),
            edges: Vec::new(),
            weight: 0.0,
        }
    }

    /// Add a vertex to the path
    pub fn add_vertex(&mut self, vertex: Vertex) {
        self.vertices.push(vertex);
    }

    /// Add an edge to the path
    pub fn add_edge(&mut self, edge: Edge) {
        self.weight += edge.weight();
        self.edges.push(edge);
    }

    /// Get path length (number of edges)
    pub fn length(&self) -> usize {
        self.edges.len()
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

        // Add all vertices first
        for edge_def in &self.definition.edge_definitions {
            for vertex_collection in edge_def.from.iter().chain(edge_def.to.iter()) {
                self.load_vertices_from_collection(vertex_collection)?;
            }
        }

        // Add orphan collections
        for orphan_collection in &self.definition.orphan_collections {
            self.load_vertices_from_collection(orphan_collection)?;
        }

        // Add all edges
        for edge_def in &self.definition.edge_definitions {
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
    pub fn traverse(&self, options: TraversalOptions) -> Result<TraversalResult> {
        let start_index = self.vertex_index_cache.get(&options.start_vertex)
            .ok_or_else(|| ArangoError::new(
                ErrorCode::GraphInvalidVertex,
                format!("Start vertex '{}' not found in graph", options.start_vertex)
            ))?;

        let mut result = TraversalResult {
            vertices: Vec::new(),
            edges: Vec::new(),
            paths: Vec::new(),
        };

        let mut visited_vertices = HashSet::new();
        let mut visited_edges = HashSet::new();
        let mut queue = VecDeque::new();

        // Initialize with start vertex
        queue.push_back((start_index.clone(), 0u32, Path::new()));

        while let Some((current_index, depth, current_path)) = queue.pop_front() {
            // Check depth limits
            if depth > options.max_depth {
                continue;
            }

            // Check limit
            if let Some(limit) = options.limit {
                if result.vertices.len() >= limit {
                    break;
                }
            }

            // Get current vertex
            let current_vertex_id = &self.petgraph[current_index];

            // Skip if already visited (for unique vertices)
            if options.unique_vertices && visited_vertices.contains(current_vertex_id) {
                continue;
            }

            // Load vertex document
            if let Ok(Some(vertex_doc)) = self.storage.get_document(
                current_vertex_id.collection(),
                current_vertex_id.key()
            ) {
                let vertex = Vertex::new(vertex_doc);

                // Apply vertex filter
                if let Some(ref filter) = options.vertex_filter {
                    if !filter(&vertex) {
                        continue;
                    }
                }

                // Add to result if within depth range
                if depth >= options.min_depth {
                    result.vertices.push(vertex.clone());
                    if !current_path.vertices.is_empty() || !current_path.edges.is_empty() {
                        let mut path = current_path.clone();
                        path.add_vertex(vertex);
                        result.paths.push(path);
                    }
                }

                visited_vertices.insert(current_vertex_id.clone());

                // Continue traversal if not at max depth
                if depth < options.max_depth {
                    self.explore_neighbors(
                        current_index,
                        &options,
                        depth + 1,
                        current_path,
                        &mut visited_edges,
                        &mut queue,
                        &mut result,
                    )?;
                }
            }
        }

        Ok(result)
    }

    /// Explore neighbors during traversal
    fn explore_neighbors(
        &self,
        current_index: NodeIndex,
        options: &TraversalOptions,
        next_depth: u32,
        mut current_path: Path,
        visited_edges: &mut HashSet<DocumentId>,
        queue: &mut VecDeque<(NodeIndex, u32, Path)>,
        result: &mut TraversalResult,
    ) -> Result<()> {
        let edges = match options.direction {
            EdgeDirection::Outbound => {
                self.petgraph.edges(current_index).map(|e| (e.target(), e.id(), e.weight())).collect()
            }
            EdgeDirection::Inbound => {
                self.petgraph.edges_directed(current_index, petgraph::Direction::Incoming)
                    .map(|e| (e.source(), e.id(), e.weight())).collect()
            }
            EdgeDirection::Any => {
                let mut all_edges = Vec::new();
                all_edges.extend(self.petgraph.edges(current_index).map(|e| (e.target(), e.id(), e.weight())));
                all_edges.extend(
                    self.petgraph.edges_directed(current_index, petgraph::Direction::Incoming)
                        .map(|e| (e.source(), e.id(), e.weight()))
                );
                all_edges
            }
        };

        for (neighbor_index, edge_index, edge_weight) in edges {
            let edge_id = edge_weight;

            // Skip if edge already visited (for unique edges)
            if options.unique_edges && visited_edges.contains(edge_id) {
                continue;
            }

            // Load edge document
            if let Ok(Some(edge_doc)) = self.storage.get_document(
                edge_id.collection(),
                edge_id.key()
            ) {
                // Create edge document (need to parse from and to from the document)
                if let (Some(from_value), Some(to_value)) = (edge_doc.get("_from"), edge_doc.get("_to")) {
                    if let (Some(from_str), Some(to_str)) = (from_value.as_str(), to_value.as_str()) {
                        if let (Ok(from_id), Ok(to_id)) = (DocumentId::parse(from_str), DocumentId::parse(to_str)) {
                            if let Ok(edge_document) = EdgeDocument::new(
                                edge_doc.collection(),
                                from_id,
                                to_id,
                                edge_doc.data_only()
                            ) {
                                let edge = Edge::new(edge_document);

                                // Apply edge filter
                                if let Some(ref filter) = options.edge_filter {
                                    if !filter(&edge) {
                                        continue;
                                    }
                                }

                                result.edges.push(edge.clone());
                                visited_edges.insert(edge_id.clone());

                                // Add to path and queue
                                let mut new_path = current_path.clone();
                                new_path.add_edge(edge);
                                queue.push_back((neighbor_index, next_depth, new_path));
                            }
                        }
                    }
                }
            }
        }

        Ok(())
    }

    /// Find shortest path between two vertices
    pub fn shortest_path(&self, options: ShortestPathOptions) -> Result<Option<Path>> {
        let start_index = self.vertex_index_cache.get(&options.start_vertex)
            .ok_or_else(|| ArangoError::new(
                ErrorCode::GraphInvalidVertex,
                format!("Start vertex '{}' not found in graph", options.start_vertex)
            ))?;

        let target_index = self.vertex_index_cache.get(&options.target_vertex)
            .ok_or_else(|| ArangoError::new(
                ErrorCode::GraphInvalidVertex,
                format!("Target vertex '{}' not found in graph", options.target_vertex)
            ))?;

        // Create weight map for edges
        let edge_weights: HashMap<EdgeIndex, f64> = self.petgraph
            .edge_indices()
            .map(|edge_index| {
                let edge_id = &self.petgraph[edge_index];
                let weight = if let Ok(Some(edge_doc)) = self.storage.get_document(
                    edge_id.collection(),
                    edge_id.key()
                ) {
                    if let Some(weight_attr) = &options.weight_attribute {
                        edge_doc.get(weight_attr)
                            .and_then(|v| v.as_f64())
                            .unwrap_or(options.default_weight)
                    } else {
                        options.default_weight
                    }
                } else {
                    options.default_weight
                };
                (edge_index, weight)
            })
            .collect();

        // Use Dijkstra's algorithm
        let path_map = dijkstra(
            &self.petgraph,
            *start_index,
            Some(*target_index),
            |edge_ref| edge_weights.get(&edge_ref.id()).copied().unwrap_or(options.default_weight)
        );

        if let Some(&total_weight) = path_map.get(target_index) {
            // Reconstruct path
            let mut path = Path::new();
            path.weight = total_weight;

            // TODO: Implement path reconstruction
            // This requires storing the predecessor information during Dijkstra

            Ok(Some(path))
        } else {
            Ok(None)
        }
    }

    /// Get graph definition
    pub fn definition(&self) -> &GraphDefinition {
        &self.definition
    }

    /// Get graph statistics
    pub fn statistics(&self) -> GraphStatistics {
        GraphStatistics {
            vertex_count: self.vertex_index_cache.len(),
            edge_count: self.edge_index_cache.len(),
            definition: self.definition.clone(),
        }
    }
}

/// Graph statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GraphStatistics {
    pub vertex_count: usize,
    pub edge_count: usize,
    pub definition: GraphDefinition,
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