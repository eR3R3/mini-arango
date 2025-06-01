use std::collections::HashMap;
use std::sync::Arc;
use parking_lot::RwLock;
use serde::{Serialize, Deserialize};
use dashmap::DashMap;
use crate::common::error::{ArangoError, Result, ErrorCode};
use crate::common::document::{Document, DocumentKey, EdgeDocument, DocumentFilter, DocumentId};
use crate::arangod::storage_engine::{StorageEngine, CollectionType};
use super::graph::{
    ArangoGraph, GraphDefinition, EdgeDefinition,
    Vertex, Edge, GraphTraversalOptions, TraversalResult,
    GraphType, GraphStatistics,
};
use super::GraphOperations;

/// Graph information for management
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GraphInfo {
    /// Graph name
    pub name: String,
    /// Graph definition
    pub definition: GraphDefinition,
    /// Creation timestamp
    pub created: chrono::DateTime<chrono::Utc>,
    /// Last modification timestamp
    pub modified: chrono::DateTime<chrono::Utc>,
    /// Graph statistics
    pub statistics: Option<GraphStatistics>,
}

impl GraphInfo {
    /// Create new graph info
    pub fn new(definition: GraphDefinition) -> Self {
        let now = chrono::Utc::now();
        GraphInfo {
            name: definition.name.clone(),
            definition,
            created: now,
            modified: now,
            statistics: None,
        }
    }

    /// Update modification time
    pub fn touch(&mut self) {
        self.modified = chrono::Utc::now();
    }
}

/// Graph manager - handles multiple graphs and their persistence
pub struct GraphManager {
    /// Storage engine reference
    storage: Arc<dyn StorageEngine>,
    /// Cached graph instances
    graphs: DashMap<String, Arc<ArangoGraph>>,
    /// Graph metadata cache
    graph_info_cache: DashMap<String, GraphInfo>,
    /// Configuration
    config: GraphManagerConfig,
}

/// Graph manager configuration
#[derive(Debug, Clone)]
pub struct GraphManagerConfig {
    /// Maximum number of graphs to keep in memory
    pub max_cached_graphs: usize,
    /// Whether to auto-load graph structure on startup
    pub auto_load_on_startup: bool,
    /// Default graph type for new graphs
    pub default_graph_type: GraphType,
}

impl Default for GraphManagerConfig {
    fn default() -> Self {
        GraphManagerConfig {
            max_cached_graphs: 100,
            auto_load_on_startup: true,
            default_graph_type: GraphType::Directed,
        }
    }
}

/// Graph metadata collection name
const GRAPH_METADATA_COLLECTION: &str = "_graphs";

impl GraphManager {
    /// Create a new graph manager
    pub fn new(storage: Arc<dyn StorageEngine>) -> Result<Self> {
        let config = GraphManagerConfig::default();

        // Ensure metadata collection exists
        if storage.get_collection(GRAPH_METADATA_COLLECTION).is_none() {
            storage.create_collection(GRAPH_METADATA_COLLECTION, CollectionType::Document)?;
        }

        let manager = GraphManager {
            storage,
            graphs: DashMap::new(),
            graph_info_cache: DashMap::new(),
            config,
        };

        // Load existing graphs if configured to do so
        if manager.config.auto_load_on_startup {
            manager.load_graph_metadata()?;
        }

        Ok(manager)
    }

    /// Create a new graph manager with custom configuration
    pub fn with_config(storage: Arc<dyn StorageEngine>, config: GraphManagerConfig) -> Result<Self> {
        // Ensure metadata collection exists
        if storage.get_collection(GRAPH_METADATA_COLLECTION).is_none() {
            storage.create_collection(GRAPH_METADATA_COLLECTION, CollectionType::Document)?;
        }

        let manager = GraphManager {
            storage,
            graphs: DashMap::new(),
            graph_info_cache: DashMap::new(),
            config,
        };

        // Load existing graphs if configured to do so
        if manager.config.auto_load_on_startup {
            manager.load_graph_metadata()?;
        }

        Ok(manager)
    }

    /// Load graph metadata from storage
    fn load_graph_metadata(&self) -> Result<()> {
        let graph_documents = self.storage.find_documents(
            GRAPH_METADATA_COLLECTION,
            &DocumentFilter::All,
            None
        )?;

        for document in graph_documents {
            if let Ok(graph_info) = serde_json::from_value::<GraphInfo>(document.to_json()) {
                self.graph_info_cache.insert(graph_info.name.clone(), graph_info);
            }
        }

        Ok(())
    }

    /// Save graph metadata to storage
    fn save_graph_metadata(&self, graph_info: &GraphInfo) -> Result<()> {
        let graph_data = serde_json::to_value(graph_info)
            .map_err(|e| ArangoError::internal(format!("Failed to serialize graph info: {}", e)))?;

        if let serde_json::Value::Object(data_map) = graph_data {
            let document = Document::with_key(
                GRAPH_METADATA_COLLECTION,
                DocumentKey::new(&graph_info.name)?,
                data_map
            );

            // Try to update first, then insert if not found
            if self.storage.update_document(GRAPH_METADATA_COLLECTION, &document).is_err() {
                self.storage.insert_document(GRAPH_METADATA_COLLECTION, &document)?;
            }
        }

        Ok(())
    }

    /// Delete graph metadata from storage
    fn delete_graph_metadata(&self, graph_name: &str) -> Result<()> {
        let key = DocumentKey::new(graph_name)?;
        self.storage.delete_document(GRAPH_METADATA_COLLECTION, &key)?;
        Ok(())
    }

    /// Get or load a graph instance
    fn get_or_load_graph(&self, graph_name: &str) -> Result<Option<Arc<ArangoGraph>>> {
        // Check if already cached
        if let Some(graph) = self.graphs.get(graph_name) {
            return Ok(Some(graph.clone()));
        }

        // Load from metadata
        if let Some(graph_info) = self.graph_info_cache.get(graph_name) {
            let graph = Arc::new(ArangoGraph::new(
                graph_info.definition.clone(),
                self.storage.clone()
            )?);

            self.graphs.insert(graph_name.to_string(), graph.clone());
            Ok(Some(graph))
        } else {
            Ok(None)
        }
    }
}

impl GraphOperations for GraphManager {
    /// Create a new graph
    fn create_graph(&self, definition: GraphDefinition) -> Result<ArangoGraph> {
        // Check if graph already exists
        if self.graph_info_cache.contains_key(&definition.name) {
            return Err(ArangoError::new(
                ErrorCode::GraphDuplicateError,
                format!("Graph '{}' already exists", definition.name)
            ));
        }

        // Create graph instance
        let graph = ArangoGraph::new(definition.clone(), self.storage.clone())?;

        // Create and save graph info
        let graph_info = GraphInfo::new(definition);
        self.save_graph_metadata(&graph_info)?;

        // Cache graph info and instance
        self.graph_info_cache.insert(graph_info.name.clone(), graph_info);
        self.graphs.insert(graph.definition().name.clone(), Arc::new(graph.clone()));

        Ok(graph)
    }

    /// Get an existing graph
    fn get_graph(&self, name: &str) -> Result<Option<&ArangoGraph>> {
        // Note: This is a simplified implementation that doesn't match the lifetime requirements
        // In a real implementation, you would need to restructure to handle lifetimes properly
        match self.get_or_load_graph(name)? {
            Some(_) => {
                // This is a workaround - in practice you'd need different data structures
                Ok(None) // Temporary implementation
            },
            None => Ok(None),
        }
    }

    /// Drop a graph
    fn drop_graph(&self, name: &str) -> Result<()> {
        // Check if graph exists
        if !self.graph_info_cache.contains_key(name) {
            return Err(ArangoError::new(
                ErrorCode::GraphNotFound,
                format!("Graph '{}' not found", name)
            ));
        }

        // Remove from caches
        self.graphs.remove(name);
        self.graph_info_cache.remove(name);

        // Remove from storage
        self.delete_graph_metadata(name)?;

        Ok(())
    }

    /// List all graphs
    fn list_graphs(&self) -> Vec<String> {
        self.graph_info_cache.iter().map(|entry| entry.key().clone()).collect()
    }

    /// Insert a vertex into a graph
    fn insert_vertex(&self, graph_name: &str, collection: &str, document: Document) -> Result<Vertex> {
        let _graph = self.get_or_load_graph(graph_name)?
            .ok_or_else(|| ArangoError::new(
                ErrorCode::GraphNotFound,
                format!("Graph '{}' not found", graph_name)
            ))?;

        // Insert document into collection
        self.storage.insert_document(collection, &document)?;

        // Create vertex
        let vertex = Vertex::new(document);

        Ok(vertex)
    }

    /// Insert an edge into a graph
    fn insert_edge(&self, graph_name: &str, collection: &str, edge_document: EdgeDocument) -> Result<Edge> {
        let _graph = self.get_or_load_graph(graph_name)?
            .ok_or_else(|| ArangoError::new(
                ErrorCode::GraphNotFound,
                format!("Graph '{}' not found", graph_name)
            ))?;

        // Insert edge document into collection
        self.storage.insert_document(collection, edge_document.document())?;

        // Create edge
        let edge = Edge::new(edge_document);

        Ok(edge)
    }

    /// Traverse a graph
    fn traverse_graph(&self, graph_name: &str, start_vertex: &str, options: GraphTraversalOptions) -> Result<super::graph::TraversalResult> {
        let start_vertex_id = DocumentId::parse(start_vertex)?;
        let graph = self.get_or_load_graph(graph_name)?
            .ok_or_else(|| ArangoError::not_found(format!("Graph '{}' not found", graph_name)))?;

        graph.traverse(&start_vertex_id, &options)
    }

    /// Find shortest path in a graph
    fn shortest_path(&self, graph_name: &str, options: GraphTraversalOptions) -> Result<Option<Vec<super::graph::PathSegment>>> {
        let graph = self.get_or_load_graph(graph_name)?
            .ok_or_else(|| ArangoError::not_found(format!("Graph '{}' not found", graph_name)))?;

        graph.shortest_path(&options)
    }

    /// Compute PageRank scores for all vertices in a graph
    fn compute_page_rank(&self, graph_name: &str, damping_factor: f64, max_iterations: usize) -> Result<std::collections::HashMap<DocumentId, f64>> {
        self.compute_page_rank(graph_name, max_iterations, damping_factor)
    }

    /// Find strongly connected components
    fn find_strongly_connected_components(&self, graph_name: &str) -> Result<Vec<Vec<DocumentId>>> {
        self.find_strongly_connected_components(graph_name)
    }

    /// Detect cycles in the graph
    fn detect_cycles(&self, graph_name: &str) -> Result<Vec<Vec<DocumentId>>> {
        self.detect_cycles(graph_name)
    }
}

impl GraphManager {
    /// Compute PageRank scores for all vertices in a graph
    pub fn compute_page_rank(&self, graph_name: &str, iterations: usize, damping_factor: f64) -> Result<HashMap<DocumentId, f64>> {
        let graph = self.get_or_load_graph(graph_name)?
            .ok_or_else(|| ArangoError::not_found(format!("Graph '{}' not found", graph_name)))?;

        // Simplified PageRank implementation
        let mut scores = HashMap::new();
        let vertices = graph.get_all_vertices().unwrap_or_default();

        // Initialize all vertices with equal scores
        let initial_score = 1.0 / vertices.len() as f64;
        for vertex in &vertices {
            scores.insert(vertex.id().clone(), initial_score);
        }

        // Iterate to compute PageRank
        for _ in 0..iterations {
            let mut new_scores = HashMap::new();

            for vertex in &vertices {
                let mut score = (1.0 - damping_factor) / vertices.len() as f64;

                // Get incoming edges and their contributions
                if let Ok(edges) = graph.get_edges_to(vertex.id()) {
                    for edge in edges {
                        if let Some(source_score) = scores.get(edge.from()) {
                            let outgoing_count = graph.get_edges_from(edge.from())
                                .map(|edges| edges.len() as f64)
                                .unwrap_or(1.0);
                            score += damping_factor * source_score / outgoing_count;
                        }
                    }
                }

                new_scores.insert(vertex.id().clone(), score);
            }

            scores = new_scores;
        }

        Ok(scores)
    }

    /// Find strongly connected components in a graph using Tarjan's algorithm
    pub fn find_strongly_connected_components(&self, graph_name: &str) -> Result<Vec<Vec<DocumentId>>> {
        let graph = self.get_or_load_graph(graph_name)?
            .ok_or_else(|| ArangoError::not_found(format!("Graph '{}' not found", graph_name)))?;

        let vertices = graph.get_all_vertices().unwrap_or_default();
        let mut components = Vec::new();
        let mut visited = std::collections::HashSet::new();
        let mut stack = Vec::new();
        let mut on_stack = std::collections::HashSet::new();
        let mut indices = HashMap::new();
        let mut lowlinks = HashMap::new();
        let mut index = 0;

        for vertex in &vertices {
            if !visited.contains(vertex.id()) {
                self.tarjan_scc(
                    &graph,
                    vertex.id(),
                    &mut index,
                    &mut visited,
                    &mut stack,
                    &mut on_stack,
                    &mut indices,
                    &mut lowlinks,
                    &mut components,
                )?;
            }
        }

        Ok(components)
    }

    /// Helper method for Tarjan's strongly connected components algorithm
    fn tarjan_scc(
        &self,
        graph: &ArangoGraph,
        v: &DocumentId,
        index: &mut usize,
        visited: &mut std::collections::HashSet<DocumentId>,
        stack: &mut Vec<DocumentId>,
        on_stack: &mut std::collections::HashSet<DocumentId>,
        indices: &mut HashMap<DocumentId, usize>,
        lowlinks: &mut HashMap<DocumentId, usize>,
        components: &mut Vec<Vec<DocumentId>>,
    ) -> Result<()> {
        indices.insert(v.clone(), *index);
        lowlinks.insert(v.clone(), *index);
        *index += 1;
        stack.push(v.clone());
        on_stack.insert(v.clone());
        visited.insert(v.clone());

        // Consider successors of v
        if let Ok(edges) = graph.get_edges_from(v) {
            for edge in edges {
                if !visited.contains(edge.to()) {
                    self.tarjan_scc(
                        graph,
                        edge.to(),
                        index,
                        visited,
                        stack,
                        on_stack,
                        indices,
                        lowlinks,
                        components,
                    )?;
                    let v_lowlink = lowlinks.get(v).copied().unwrap_or(0);
                    let to_lowlink = lowlinks.get(edge.to()).copied().unwrap_or(0);
                    lowlinks.insert(v.clone(), v_lowlink.min(to_lowlink));
                } else if on_stack.contains(edge.to()) {
                    let v_lowlink = lowlinks.get(v).copied().unwrap_or(0);
                    let to_index = indices.get(edge.to()).copied().unwrap_or(0);
                    lowlinks.insert(v.clone(), v_lowlink.min(to_index));
                }
            }
        }

        // If v is a root node, pop the stack and create component
        if lowlinks.get(v) == indices.get(v) {
            let mut component = Vec::new();
            loop {
                if let Some(w) = stack.pop() {
                    on_stack.remove(&w);
                    component.push(w.clone());
                    if w == *v {
                        break;
                    }
                } else {
                    break;
                }
            }
            if !component.is_empty() {
                components.push(component);
            }
        }

        Ok(())
    }

    /// Detect cycles in a graph using DFS
    pub fn detect_cycles(&self, graph_name: &str) -> Result<Vec<Vec<DocumentId>>> {
        let graph = self.get_or_load_graph(graph_name)?
            .ok_or_else(|| ArangoError::not_found(format!("Graph '{}' not found", graph_name)))?;

        let vertices = graph.get_all_vertices().unwrap_or_default();
        let mut cycles = Vec::new();
        let mut visited = std::collections::HashSet::new();
        let mut rec_stack = std::collections::HashSet::new();
        let mut path = Vec::new();

        for vertex in &vertices {
            if !visited.contains(vertex.id()) {
                self.dfs_cycle_detection(
                    &graph,
                    vertex.id(),
                    &mut visited,
                    &mut rec_stack,
                    &mut path,
                    &mut cycles,
                )?;
            }
        }

        Ok(cycles)
    }

    /// Helper method for cycle detection using DFS
    fn dfs_cycle_detection(
        &self,
        graph: &ArangoGraph,
        v: &DocumentId,
        visited: &mut std::collections::HashSet<DocumentId>,
        rec_stack: &mut std::collections::HashSet<DocumentId>,
        path: &mut Vec<DocumentId>,
        cycles: &mut Vec<Vec<DocumentId>>,
    ) -> Result<()> {
        visited.insert(v.clone());
        rec_stack.insert(v.clone());
        path.push(v.clone());

        if let Ok(edges) = graph.get_edges_from(v) {
            for edge in edges {
                if !visited.contains(edge.to()) {
                    self.dfs_cycle_detection(graph, edge.to(), visited, rec_stack, path, cycles)?;
                } else if rec_stack.contains(edge.to()) {
                    // Found a back edge, extract cycle
                    if let Some(start_pos) = path.iter().position(|id| *id == *edge.to()) {
                        let cycle = path[start_pos..].to_vec();
                        cycles.push(cycle);
                    }
                }
            }
        }

        rec_stack.remove(v);
        path.pop();
        Ok(())
    }
} 