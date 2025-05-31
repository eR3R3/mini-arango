use std::collections::{HashMap, HashSet, VecDeque, BinaryHeap};
use std::cmp::Ordering;
use std::f64::INFINITY;
use crate::common::error::{ArangoError, Result};
use crate::common::document::DocumentId;
use super::graph::{ArangoGraph, Path, Vertex, Edge, EdgeDirection, TraversalOptions};

/// Priority queue item for shortest path algorithms
#[derive(Debug, Clone)]
struct PathItem {
    cost: f64,
    vertex_id: DocumentId,
    path: Vec<DocumentId>,
}

impl Eq for PathItem {}

impl PartialEq for PathItem {
    fn eq(&self, other: &Self) -> bool {
        self.cost == other.cost && self.vertex_id == other.vertex_id
    }
}

impl Ord for PathItem {
    fn cmp(&self, other: &Self) -> Ordering {
        // Reverse ordering for min-heap
        other.cost.partial_cmp(&self.cost).unwrap_or(Ordering::Equal)
    }
}

impl PartialOrd for PathItem {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// Advanced graph algorithms implementation
pub struct GraphAlgorithms;

impl GraphAlgorithms {
    /// Find all shortest paths between two vertices using Dijkstra's algorithm
    pub fn dijkstra_shortest_path(
        graph: &ArangoGraph,
        start: &DocumentId,
        target: &DocumentId,
        direction: EdgeDirection,
        weight_attribute: Option<&str>,
    ) -> Result<Option<Path>> {
        let mut distances: HashMap<DocumentId, f64> = HashMap::new();
        let mut previous: HashMap<DocumentId, DocumentId> = HashMap::new();
        let mut heap = BinaryHeap::new();
        let mut visited = HashSet::new();

        // Initialize
        distances.insert(start.clone(), 0.0);
        heap.push(PathItem {
            cost: 0.0,
            vertex_id: start.clone(),
            path: vec![start.clone()],
        });

        while let Some(PathItem { cost, vertex_id, path }) = heap.pop() {
            // Skip if already visited
            if visited.contains(&vertex_id) {
                continue;
            }

            visited.insert(vertex_id.clone());

            // Found target
            if vertex_id == *target {
                return Self::reconstruct_path(graph, &path, weight_attribute);
            }

            // Explore neighbors
            let neighbors = Self::get_neighbors(graph, &vertex_id, direction)?;

            for (neighbor_id, edge_weight) in neighbors {
                if visited.contains(&neighbor_id) {
                    continue;
                }

                let new_cost = cost + edge_weight;
                let current_distance = distances.get(&neighbor_id).copied().unwrap_or(INFINITY);

                if new_cost < current_distance {
                    distances.insert(neighbor_id.clone(), new_cost);
                    previous.insert(neighbor_id.clone(), vertex_id.clone());

                    let mut new_path = path.clone();
                    new_path.push(neighbor_id.clone());

                    heap.push(PathItem {
                        cost: new_cost,
                        vertex_id: neighbor_id,
                        path: new_path,
                    });
                }
            }
        }

        Ok(None) // No path found
    }

    /// Find k shortest paths using Yen's algorithm
    pub fn k_shortest_paths(
        graph: &ArangoGraph,
        start: &DocumentId,
        target: &DocumentId,
        k: usize,
        direction: EdgeDirection,
        weight_attribute: Option<&str>,
    ) -> Result<Vec<Path>> {
        let mut result_paths = Vec::new();
        let mut potential_paths = BinaryHeap::new();

        // Find the shortest path
        if let Some(shortest) = Self::dijkstra_shortest_path(graph, start, target, direction, weight_attribute)? {
            result_paths.push(shortest.clone());

            // Generate k-1 additional paths
            for i in 1..k {
                let last_path = &result_paths[i - 1];

                // For each node in the last path, find alternate routes
                for j in 0..last_path.vertices.len().saturating_sub(1) {
                    let spur_node = &last_path.vertices[j].id();
                    let root_path = &last_path.vertices[0..=j];

                    // Remove edges and vertices used in previous paths
                    let mut modified_graph = graph.clone_structure();

                    // This is a simplified implementation
                    // In a real implementation, we'd temporarily remove edges/vertices
                    // and restore them after finding the spur path

                    if let Some(spur_path) = Self::dijkstra_shortest_path(
                        &modified_graph,
                        spur_node,
                        target,
                        direction,
                        weight_attribute
                    )? {
                        // Combine root path and spur path
                        let total_path = Self::combine_paths(root_path, &spur_path)?;

                        // Add to potential paths if not already found
                        if !Self::path_exists_in_results(&result_paths, &total_path) {
                            potential_paths.push((total_path.weight, total_path));
                        }
                    }
                }

                // Get the shortest potential path
                if let Some((_, path)) = potential_paths.pop() {
                    result_paths.push(path);
                } else {
                    break; // No more paths available
                }
            }
        }

        Ok(result_paths)
    }

    /// Breadth-First Search traversal
    pub fn bfs_traversal(
        graph: &ArangoGraph,
        start: &DocumentId,
        direction: EdgeDirection,
        max_depth: Option<u32>,
        visitor: &mut dyn FnMut(&DocumentId, u32) -> bool, // return false to stop
    ) -> Result<()> {
        let mut visited = HashSet::new();
        let mut queue = VecDeque::new();

        queue.push_back((start.clone(), 0));
        visited.insert(start.clone());

        while let Some((vertex_id, depth)) = queue.pop_front() {
            // Check depth limit
            if let Some(max_d) = max_depth {
                if depth >= max_d {
                    continue;
                }
            }

            // Visit vertex
            if !visitor(&vertex_id, depth) {
                break;
            }

            // Add neighbors to queue
            let neighbors = Self::get_neighbors(graph, &vertex_id, direction)?;
            for (neighbor_id, _) in neighbors {
                if !visited.contains(&neighbor_id) {
                    visited.insert(neighbor_id.clone());
                    queue.push_back((neighbor_id, depth + 1));
                }
            }
        }

        Ok(())
    }

    /// Depth-First Search traversal
    pub fn dfs_traversal(
        graph: &ArangoGraph,
        start: &DocumentId,
        direction: EdgeDirection,
        max_depth: Option<u32>,
        visitor: &mut dyn FnMut(&DocumentId, u32) -> bool, // return false to stop
    ) -> Result<()> {
        let mut visited = HashSet::new();
        let mut stack = Vec::new();

        stack.push((start.clone(), 0));

        while let Some((vertex_id, depth)) = stack.pop() {
            // Check if already visited
            if visited.contains(&vertex_id) {
                continue;
            }

            // Check depth limit
            if let Some(max_d) = max_depth {
                if depth >= max_d {
                    continue;
                }
            }

            visited.insert(vertex_id.clone());

            // Visit vertex
            if !visitor(&vertex_id, depth) {
                break;
            }

            // Add neighbors to stack (in reverse order for left-to-right traversal)
            let mut neighbors = Self::get_neighbors(graph, &vertex_id, direction)?;
            neighbors.reverse();

            for (neighbor_id, _) in neighbors {
                if !visited.contains(&neighbor_id) {
                    stack.push((neighbor_id, depth + 1));
                }
            }
        }

        Ok(())
    }

    /// Find strongly connected components using Tarjan's algorithm
    pub fn strongly_connected_components(graph: &ArangoGraph) -> Result<Vec<Vec<DocumentId>>> {
        let mut index = 0;
        let mut stack = Vec::new();
        let mut indices: HashMap<DocumentId, usize> = HashMap::new();
        let mut lowlinks: HashMap<DocumentId, usize> = HashMap::new();
        let mut on_stack: HashSet<DocumentId> = HashSet::new();
        let mut components = Vec::new();

        // Get all vertices
        let vertices = graph.get_all_vertices()?;

        for vertex_id in vertices {
            if !indices.contains_key(&vertex_id) {
                Self::tarjan_strongconnect(
                    graph,
                    &vertex_id,
                    &mut index,
                    &mut stack,
                    &mut indices,
                    &mut lowlinks,
                    &mut on_stack,
                    &mut components,
                )?;
            }
        }

        Ok(components)
    }

    /// Page Rank algorithm implementation
    pub fn page_rank(
        graph: &ArangoGraph,
        damping_factor: f64,
        max_iterations: usize,
        tolerance: f64,
    ) -> Result<HashMap<DocumentId, f64>> {
        let vertices = graph.get_all_vertices()?;
        let vertex_count = vertices.len();
        let initial_rank = 1.0 / vertex_count as f64;

        let mut ranks: HashMap<DocumentId, f64> = vertices.iter()
            .map(|v| (v.clone(), initial_rank))
            .collect();

        let mut new_ranks = ranks.clone();

        for _ in 0..max_iterations {
            let mut converged = true;

            for vertex_id in &vertices {
                let mut rank_sum = 0.0;

                // Get incoming neighbors
                let incoming = Self::get_neighbors(graph, vertex_id, EdgeDirection::Inbound)?;

                for (neighbor_id, _) in incoming {
                    let neighbor_rank = ranks.get(&neighbor_id).copied().unwrap_or(0.0);
                    let outgoing_count = Self::get_neighbors(graph, &neighbor_id, EdgeDirection::Outbound)?.len();

                    if outgoing_count > 0 {
                        rank_sum += neighbor_rank / outgoing_count as f64;
                    }
                }

                let new_rank = (1.0 - damping_factor) / vertex_count as f64 + damping_factor * rank_sum;

                if (new_rank - ranks.get(vertex_id).copied().unwrap_or(0.0)).abs() > tolerance {
                    converged = false;
                }

                new_ranks.insert(vertex_id.clone(), new_rank);
            }

            ranks = new_ranks.clone();

            if converged {
                break;
            }
        }

        Ok(ranks)
    }

    /// Detect cycles in the graph using DFS
    pub fn detect_cycles(graph: &ArangoGraph) -> Result<Vec<Vec<DocumentId>>> {
        let mut visited = HashSet::new();
        let mut rec_stack = HashSet::new();
        let mut cycles = Vec::new();
        let mut current_path = Vec::new();

        let vertices = graph.get_all_vertices()?;

        for vertex_id in vertices {
            if !visited.contains(&vertex_id) {
                Self::dfs_cycle_detect(
                    graph,
                    &vertex_id,
                    &mut visited,
                    &mut rec_stack,
                    &mut current_path,
                    &mut cycles,
                )?;
            }
        }

        Ok(cycles)
    }

    /// Helper function for Tarjan's strongly connected components algorithm
    fn tarjan_strongconnect(
        graph: &ArangoGraph,
        vertex_id: &DocumentId,
        index: &mut usize,
        stack: &mut Vec<DocumentId>,
        indices: &mut HashMap<DocumentId, usize>,
        lowlinks: &mut HashMap<DocumentId, usize>,
        on_stack: &mut HashSet<DocumentId>,
        components: &mut Vec<Vec<DocumentId>>,
    ) -> Result<()> {
        indices.insert(vertex_id.clone(), *index);
        lowlinks.insert(vertex_id.clone(), *index);
        *index += 1;
        stack.push(vertex_id.clone());
        on_stack.insert(vertex_id.clone());

        let neighbors = Self::get_neighbors(graph, vertex_id, EdgeDirection::Outbound)?;

        for (neighbor_id, _) in neighbors {
            if !indices.contains_key(&neighbor_id) {
                Self::tarjan_strongconnect(
                    graph, &neighbor_id, index, stack, indices, lowlinks, on_stack, components
                )?;
                let neighbor_lowlink = lowlinks.get(&neighbor_id).copied().unwrap_or(0);
                let current_lowlink = lowlinks.get(vertex_id).copied().unwrap_or(0);
                lowlinks.insert(vertex_id.clone(), current_lowlink.min(neighbor_lowlink));
            } else if on_stack.contains(&neighbor_id) {
                let neighbor_index = indices.get(&neighbor_id).copied().unwrap_or(0);
                let current_lowlink = lowlinks.get(vertex_id).copied().unwrap_or(0);
                lowlinks.insert(vertex_id.clone(), current_lowlink.min(neighbor_index));
            }
        }

        // If vertex_id is a root node, pop the stack and create an SCC
        if lowlinks.get(vertex_id) == indices.get(vertex_id) {
            let mut component = Vec::new();
            loop {
                if let Some(w) = stack.pop() {
                    on_stack.remove(&w);
                    component.push(w.clone());
                    if w == *vertex_id {
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

    /// Helper function for cycle detection
    fn dfs_cycle_detect(
        graph: &ArangoGraph,
        vertex_id: &DocumentId,
        visited: &mut HashSet<DocumentId>,
        rec_stack: &mut HashSet<DocumentId>,
        current_path: &mut Vec<DocumentId>,
        cycles: &mut Vec<Vec<DocumentId>>,
    ) -> Result<()> {
        visited.insert(vertex_id.clone());
        rec_stack.insert(vertex_id.clone());
        current_path.push(vertex_id.clone());

        let neighbors = Self::get_neighbors(graph, vertex_id, EdgeDirection::Outbound)?;

        for (neighbor_id, _) in neighbors {
            if !visited.contains(&neighbor_id) {
                Self::dfs_cycle_detect(graph, &neighbor_id, visited, rec_stack, current_path, cycles)?;
            } else if rec_stack.contains(&neighbor_id) {
                // Found a cycle
                if let Some(cycle_start) = current_path.iter().position(|v| v == &neighbor_id) {
                    let cycle = current_path[cycle_start..].to_vec();
                    cycles.push(cycle);
                }
            }
        }

        rec_stack.remove(vertex_id);
        current_path.pop();

        Ok(())
    }

    /// Get neighbors of a vertex with their edge weights
    fn get_neighbors(
        graph: &ArangoGraph,
        vertex_id: &DocumentId,
        direction: EdgeDirection,
    ) -> Result<Vec<(DocumentId, f64)>> {
        // This would need to be implemented in the ArangoGraph
        // For now, returning empty vector
        Ok(Vec::new())
    }

    /// Reconstruct path from vertex sequence
    fn reconstruct_path(
        graph: &ArangoGraph,
        vertex_ids: &[DocumentId],
        weight_attribute: Option<&str>,
    ) -> Result<Option<Path>> {
        let mut path = Path::new();

        for vertex_id in vertex_ids {
            if let Ok(Some(document)) = graph.get_vertex_document(vertex_id) {
                path.add_vertex(Vertex::new(document));
            }
        }

        // Add edges between consecutive vertices
        for i in 0..vertex_ids.len().saturating_sub(1) {
            if let Ok(Some(edge_doc)) = graph.get_edge_between(&vertex_ids[i], &vertex_ids[i + 1]) {
                let edge = Edge::new(edge_doc);
                path.add_edge(edge);
            }
        }

        Ok(Some(path))
    }

    /// Check if path already exists in results
    fn path_exists_in_results(results: &[Path], path: &Path) -> bool {
        results.iter().any(|existing| {
            existing.vertices.len() == path.vertices.len() &&
                existing.vertices.iter().zip(path.vertices.iter())
                    .all(|(v1, v2)| v1.id() == v2.id())
        })
    }

    /// Combine two path segments
    fn combine_paths(root: &[Vertex], spur: &Path) -> Result<Path> {
        let mut combined = Path::new();

        // Add root vertices
        for vertex in root {
            combined.add_vertex(vertex.clone());
        }

        // Add spur vertices (skip first if it overlaps with root)
        let start_idx = if !spur.vertices.is_empty() && !root.is_empty() &&
            spur.vertices[0].id() == root.last().unwrap().id() { 1 } else { 0 };

        for vertex in &spur.vertices[start_idx..] {
            combined.add_vertex(vertex.clone());
        }

        // Add spur edges
        for edge in &spur.edges {
            combined.add_edge(edge.clone());
        }

        Ok(combined)
    }
}

/// Graph caching and optimization traits
pub trait GraphCache {
    /// Cache a traversal result
    fn cache_traversal(&mut self, options: &TraversalOptions, result: &super::graph::TraversalResult);

    /// Get cached traversal result
    fn get_cached_traversal(&self, options: &TraversalOptions) -> Option<&super::graph::TraversalResult>;

    /// Invalidate cache for a vertex
    fn invalidate_vertex(&mut self, vertex_id: &DocumentId);

    /// Clear all cache
    fn clear_cache(&mut self);
}

/// Simple in-memory graph cache implementation
pub struct MemoryGraphCache {
    traversal_cache: HashMap<String, super::graph::TraversalResult>,
    cache_size_limit: usize,
}

impl MemoryGraphCache {
    pub fn new(cache_size_limit: usize) -> Self {
        MemoryGraphCache {
            traversal_cache: HashMap::new(),
            cache_size_limit,
        }
    }

    fn generate_cache_key(options: &TraversalOptions) -> String {
        format!("{}_{:?}_{}_{}",
                options.start_vertex,
                options.direction,
                options.min_depth,
                options.max_depth
        )
    }
}

impl GraphCache for MemoryGraphCache {
    fn cache_traversal(&mut self, options: &TraversalOptions, result: &super::graph::TraversalResult) {
        if self.traversal_cache.len() >= self.cache_size_limit {
            // Simple LRU: remove first entry
            if let Some(key) = self.traversal_cache.keys().next().cloned() {
                self.traversal_cache.remove(&key);
            }
        }

        let key = Self::generate_cache_key(options);
        self.traversal_cache.insert(key, result.clone());
    }

    fn get_cached_traversal(&self, options: &TraversalOptions) -> Option<&super::graph::TraversalResult> {
        let key = Self::generate_cache_key(options);
        self.traversal_cache.get(&key)
    }

    fn invalidate_vertex(&mut self, vertex_id: &DocumentId) {
        // Remove all cached traversals that might be affected by this vertex
        let keys_to_remove: Vec<String> = self.traversal_cache.keys()
            .filter(|key| key.contains(&vertex_id.to_string()))
            .cloned()
            .collect();

        for key in keys_to_remove {
            self.traversal_cache.remove(&key);
        }
    }

    fn clear_cache(&mut self) {
        self.traversal_cache.clear();
    }
} 