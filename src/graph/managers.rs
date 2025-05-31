use std::collections::HashMap;
use std::sync::Arc;
use serde::{Serialize, Deserialize};
use dashmap::DashMap;
use crate::common::error::{ArangoError, Result, ErrorCode};
use crate::common::document::{Document, DocumentKey, EdgeDocument, DocumentFilter};
use crate::arangod::storage_engine::{StorageEngine, CollectionType};
use super::graph::{
    ArangoGraph, GraphDefinition, GraphType, EdgeDefinition,
    Vertex, Edge, TraversalOptions, TraversalResult, Path,
    ShortestPathOptions, GraphStatistics
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
    fn get_graph(&self, name: &str) -> Result<Option<ArangoGraph>> {
        match self.get_or_load_graph(name)? {
            Some(graph) => Ok(Some((*graph).clone())),
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
    fn list_graphs(&self) -> Result<Vec<GraphInfo>> {
        Ok(self.graph_info_cache.iter().map(|entry| entry.value().clone()).collect())
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
    fn traverse_graph(&self, graph_name: &str, options: TraversalOptions) -> Result<TraversalResult> {
        let graph = self.get_or_load_graph(graph_name)?
            .ok_or_else(|| ArangoError::new(
                ErrorCode::GraphNotFound,
                format!("Graph '{}' not found", graph_name)
            ))?;

        graph.traverse(options)
    }

    /// Find shortest path in a graph
    fn shortest_path(&self, graph_name: &str, options: ShortestPathOptions) -> Result<Option<Path>> {
        let graph = self.get_or_load_graph(graph_name)?
            .ok_or_else(|| ArangoError::new(
                ErrorCode::GraphNotFound,
                format!("Graph '{}' not found", graph_name)
            ))?;

        graph.shortest_path(options)
    }
}