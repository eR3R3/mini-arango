use std::sync::Arc;
use crate::common::error::{ArangoError, Result};
use super::indexes::{IndexDefinition, IndexType};
use super::geo_index::GeoIndex;

/// Trait that all index implementations must implement
pub trait IndexImplementation: Send + Sync {
    /// Insert a document into the index
    fn insert(&mut self, key: &[u8], document_ref: &[u8]) -> Result<()>;

    /// Remove a document from the index
    fn remove(&mut self, key: &[u8], document_ref: &[u8]) -> Result<bool>;

    /// Lookup documents by key
    fn lookup(&self, key: &[u8]) -> Option<Vec<Vec<u8>>>;

    /// Get index definition
    fn definition(&self) -> &IndexDefinition;

    /// Calculate query costs
    fn calculate_costs(&self, query_type: &str, complexity: f64) -> f64;

    /// Check if index supports given operation
    fn supports_operation(&self, operation: &str) -> bool;
}

/// Basic key-value index for simple operations
pub struct BasicIndex {
    definition: IndexDefinition,
    data: std::collections::HashMap<Vec<u8>, Vec<Vec<u8>>>,
}

impl BasicIndex {
    pub fn new(definition: IndexDefinition) -> Self {
        BasicIndex {
            definition,
            data: std::collections::HashMap::new(),
        }
    }
}

impl IndexImplementation for BasicIndex {
    fn insert(&mut self, key: &[u8], document_ref: &[u8]) -> Result<()> {
        self.data.entry(key.to_vec())
            .or_insert_with(Vec::new)
            .push(document_ref.to_vec());
        Ok(())
    }

    fn remove(&mut self, key: &[u8], document_ref: &[u8]) -> Result<bool> {
        if let Some(refs) = self.data.get_mut(key) {
            refs.retain(|r| r != document_ref);
            if refs.is_empty() {
                self.data.remove(key);
            }
            Ok(true)
        } else {
            Ok(false)
        }
    }

    fn lookup(&self, key: &[u8]) -> Option<Vec<Vec<u8>>> {
        self.data.get(key).cloned()
    }

    fn definition(&self) -> &IndexDefinition {
        &self.definition
    }

    fn calculate_costs(&self, query_type: &str, _complexity: f64) -> f64 {
        match self.definition.index_type {
            IndexType::Hash => 1.0,  // O(1)
            IndexType::Skiplist | IndexType::Persistent => (self.data.len() as f64).log2(), // O(log n)
            _ => self.data.len() as f64, // O(n) fallback
        }
    }

    fn supports_operation(&self, operation: &str) -> bool {
        match self.definition.index_type {
            IndexType::Hash => matches!(operation, "eq" | "in"),
            IndexType::Skiplist | IndexType::Persistent => {
                matches!(operation, "eq" | "in" | "range" | "gt" | "gte" | "lt" | "lte")
            },
            _ => matches!(operation, "eq"),
        }
    }
}

/// Vector index implementation for similarity search
pub struct VectorIndex {
    definition: IndexDefinition,
    vectors: Vec<(Vec<f32>, Vec<u8>)>, // (vector, document_ref)
    dimensions: usize,
    similarity_metric: SimilarityMetric,
}

#[derive(Debug, Clone)]
pub enum SimilarityMetric {
    Cosine,
    Euclidean,
    DotProduct,
}

impl VectorIndex {
    pub fn new(definition: IndexDefinition) -> Result<Self> {
        let dimensions = definition.vector_dimensions
            .ok_or_else(|| ArangoError::InvalidIndexDefinition(
                "Vector index requires dimensions".to_string()
            ))? as usize;

        let similarity_metric = match definition.vector_similarity.as_deref() {
            Some("cosine") => SimilarityMetric::Cosine,
            Some("euclidean") => SimilarityMetric::Euclidean,
            Some("dot_product") => SimilarityMetric::DotProduct,
            _ => SimilarityMetric::Cosine, // default
        };

        Ok(VectorIndex {
            definition,
            vectors: Vec::new(),
            dimensions,
            similarity_metric,
        })
    }

    /// Calculate similarity between two vectors
    pub fn calculate_similarity(&self, v1: &[f32], v2: &[f32]) -> f64 {
        if v1.len() != v2.len() || v1.len() != self.dimensions {
            return 0.0;
        }

        match self.similarity_metric {
            SimilarityMetric::Cosine => self.cosine_similarity(v1, v2),
            SimilarityMetric::Euclidean => self.euclidean_distance(v1, v2),
            SimilarityMetric::DotProduct => self.dot_product(v1, v2),
        }
    }

    fn cosine_similarity(&self, v1: &[f32], v2: &[f32]) -> f64 {
        let dot_product: f32 = v1.iter().zip(v2.iter()).map(|(a, b)| a * b).sum();
        let magnitude1: f32 = v1.iter().map(|x| x * x).sum::<f32>().sqrt();
        let magnitude2: f32 = v2.iter().map(|x| x * x).sum::<f32>().sqrt();

        if magnitude1 == 0.0 || magnitude2 == 0.0 {
            0.0
        } else {
            (dot_product / (magnitude1 * magnitude2)) as f64
        }
    }

    fn euclidean_distance(&self, v1: &[f32], v2: &[f32]) -> f64 {
        let distance: f32 = v1.iter().zip(v2.iter())
            .map(|(a, b)| (a - b).powi(2))
            .sum::<f32>()
            .sqrt();
        // Convert distance to similarity (smaller distance = higher similarity)
        1.0 / (1.0 + distance as f64)
    }

    fn dot_product(&self, v1: &[f32], v2: &[f32]) -> f64 {
        v1.iter().zip(v2.iter()).map(|(a, b)| a * b).sum::<f32>() as f64
    }

    /// Find k nearest neighbors
    pub fn find_similar(&self, query_vector: &[f32], k: usize) -> Vec<(f64, Vec<u8>)> {
        let mut similarities: Vec<(f64, Vec<u8>)> = self.vectors
            .iter()
            .map(|(vec, doc_ref)| {
                let similarity = self.calculate_similarity(query_vector, vec);
                (similarity, doc_ref.clone())
            })
            .collect();

        // Sort by similarity (descending)
        similarities.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap_or(std::cmp::Ordering::Equal));

        // Return top k
        similarities.into_iter().take(k).collect()
    }
}

impl IndexImplementation for VectorIndex {
    fn insert(&mut self, key: &[u8], document_ref: &[u8]) -> Result<()> {
        // Parse key as vector (assuming it's serialized f32 array)
        if key.len() % 4 != 0 {
            return Err(ArangoError::InvalidIndexData("Invalid vector data".to_string()));
        }

        let vector: Vec<f32> = key.chunks_exact(4)
            .map(|chunk| f32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]))
            .collect();

        if vector.len() != self.dimensions {
            return Err(ArangoError::InvalidIndexData(
                format!("Vector dimension mismatch: expected {}, got {}",
                        self.dimensions, vector.len())
            ));
        }

        self.vectors.push((vector, document_ref.to_vec()));
        Ok(())
    }

    fn remove(&mut self, key: &[u8], document_ref: &[u8]) -> Result<bool> {
        let vector: Vec<f32> = key.chunks_exact(4)
            .map(|chunk| f32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]))
            .collect();

        let original_len = self.vectors.len();
        self.vectors.retain(|(v, doc_ref)| v != &vector || doc_ref != document_ref);

        Ok(self.vectors.len() < original_len)
    }

    fn lookup(&self, key: &[u8]) -> Option<Vec<Vec<u8>>> {
        // For vector index, exact lookup doesn't make much sense
        // This would typically be used for similarity search
        let vector: Vec<f32> = key.chunks_exact(4)
            .map(|chunk| f32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]))
            .collect();

        // Return exact matches (rare for vectors)
        let matches: Vec<Vec<u8>> = self.vectors
            .iter()
            .filter(|(v, _)| v == &vector)
            .map(|(_, doc_ref)| doc_ref.clone())
            .collect();

        if matches.is_empty() { None } else { Some(matches) }
    }

    fn definition(&self) -> &IndexDefinition {
        &self.definition
    }

    fn calculate_costs(&self, query_type: &str, _complexity: f64) -> f64 {
        match query_type {
            "similarity" => self.vectors.len() as f64, // Linear scan for now
            "knn" => self.vectors.len() as f64, // Could be optimized with HNSW
            _ => f64::INFINITY, // Unsupported operation
        }
    }

    fn supports_operation(&self, operation: &str) -> bool {
        matches!(operation, "similarity" | "knn" | "vector_search")
    }
}

/// Fulltext index implementation
pub struct FulltextIndex {
    definition: IndexDefinition,
    inverted_index: std::collections::HashMap<String, Vec<Vec<u8>>>, // term -> document_refs
    min_word_length: usize,
}

impl FulltextIndex {
    pub fn new(definition: IndexDefinition) -> Self {
        let min_word_length = definition.min_word_length.unwrap_or(2) as usize;

        FulltextIndex {
            definition,
            inverted_index: std::collections::HashMap::new(),
            min_word_length,
        }
    }

    /// Tokenize text into searchable terms
    fn tokenize(&self, text: &str) -> Vec<String> {
        text.split_whitespace()
            .map(|word| word.to_lowercase().trim_matches(|c: char| !c.is_alphanumeric()).to_string())
            .filter(|word| word.len() >= self.min_word_length)
            .collect()
    }

    /// Search for documents containing the given terms
    pub fn search_text(&self, query: &str) -> Vec<Vec<u8>> {
        let terms = self.tokenize(query);
        if terms.is_empty() {
            return Vec::new();
        }

        // Start with documents containing the first term
        let mut result_docs = self.inverted_index.get(&terms[0])
            .cloned()
            .unwrap_or_default();

        // Intersect with documents containing other terms (AND operation)
        for term in &terms[1..] {
            if let Some(term_docs) = self.inverted_index.get(term) {
                result_docs.retain(|doc| term_docs.contains(doc));
            } else {
                // Term not found, no intersection possible
                result_docs.clear();
                break;
            }
        }

        result_docs
    }
}

impl IndexImplementation for FulltextIndex {
    fn insert(&mut self, key: &[u8], document_ref: &[u8]) -> Result<()> {
        let text = String::from_utf8_lossy(key);
        let terms = self.tokenize(&text);

        for term in terms {
            self.inverted_index.entry(term)
                .or_insert_with(Vec::new)
                .push(document_ref.to_vec());
        }

        Ok(())
    }

    fn remove(&mut self, key: &[u8], document_ref: &[u8]) -> Result<bool> {
        let text = String::from_utf8_lossy(key);
        let terms = self.tokenize(&text);
        let mut removed = false;

        for term in terms {
            if let Some(docs) = self.inverted_index.get_mut(&term) {
                let original_len = docs.len();
                docs.retain(|doc| doc != document_ref);
                if docs.len() < original_len {
                    removed = true;
                }
                if docs.is_empty() {
                    self.inverted_index.remove(&term);
                }
            }
        }

        Ok(removed)
    }

    fn lookup(&self, key: &[u8]) -> Option<Vec<Vec<u8>>> {
        let query = String::from_utf8_lossy(key);
        let results = self.search_text(&query);
        if results.is_empty() { None } else { Some(results) }
    }

    fn definition(&self) -> &IndexDefinition {
        &self.definition
    }

    fn calculate_costs(&self, query_type: &str, complexity: f64) -> f64 {
        match query_type {
            "text_search" => {
                // Cost depends on number of terms and their frequency
                self.inverted_index.len() as f64 * complexity
            },
            _ => f64::INFINITY,
        }
    }

    fn supports_operation(&self, operation: &str) -> bool {
        matches!(operation, "text_search" | "fulltext" | "contains")
    }
}

/// Index factory for creating appropriate index implementations
pub struct IndexFactory;

impl IndexFactory {
    pub fn create_index(definition: IndexDefinition) -> Result<Box<dyn IndexImplementation>> {
        match definition.index_type {
            IndexType::Vector => {
                Ok(Box::new(VectorIndex::new(definition)?))
            },
            IndexType::Fulltext => {
                Ok(Box::new(FulltextIndex::new(definition)))
            },
            IndexType::Geo | IndexType::Geo1 | IndexType::Geo2 => {
                // Note: GeoIndex would need to implement IndexImplementation trait
                // For now, we'll use BasicIndex as fallback
                Ok(Box::new(BasicIndex::new(definition)))
            },
            _ => {
                // Default to BasicIndex for Hash, Skiplist, Persistent, etc.
                Ok(Box::new(BasicIndex::new(definition)))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_vector_similarity() {
        let definition = IndexDefinition {
            id: "test_vector".to_string(),
            name: "test_vector_index".to_string(),
            index_type: IndexType::Vector,
            fields: vec!["embedding".to_string()],
            unique: false,
            sparse: false,
            deduplicate: None,
            estimates: None,
            min_length: None,
            geo_json: None,
            constraint: None,
            expansion_limit: None,
            stored_values: None,
            vector_dimensions: Some(3),
            vector_similarity: Some("cosine".to_string()),
            min_word_length: None,
            expire_after: None,
            inverted_index_analyzer: None,
            inverted_index_cache: None,
        };

        let vector_index = VectorIndex::new(definition).unwrap();

        let v1 = vec![1.0, 0.0, 0.0];
        let v2 = vec![0.0, 1.0, 0.0];
        let v3 = vec![1.0, 0.0, 0.0]; // Same as v1

        // Cosine similarity tests
        assert_eq!(vector_index.calculate_similarity(&v1, &v3), 1.0); // Identical vectors
        assert_eq!(vector_index.calculate_similarity(&v1, &v2), 0.0); // Orthogonal vectors
    }

    #[test]
    fn test_fulltext_tokenization() {
        let definition = IndexDefinition {
            id: "test_fulltext".to_string(),
            name: "test_fulltext_index".to_string(),
            index_type: IndexType::Fulltext,
            fields: vec!["content".to_string()],
            unique: false,
            sparse: false,
            deduplicate: None,
            estimates: None,
            min_length: None,
            geo_json: None,
            constraint: None,
            expansion_limit: None,
            stored_values: None,
            vector_dimensions: None,
            vector_similarity: None,
            min_word_length: Some(3),
            expire_after: None,
            inverted_index_analyzer: None,
            inverted_index_cache: None,
        };

        let fulltext_index = FulltextIndex::new(definition);

        let tokens = fulltext_index.tokenize("Hello, World! This is a test.");
        assert!(tokens.contains(&"hello".to_string()));
        assert!(tokens.contains(&"world".to_string()));
        assert!(tokens.contains(&"test".to_string()));
        assert!(!tokens.contains(&"is".to_string())); // Too short (min_word_length = 3)
    }
} 