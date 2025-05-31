use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use serde::{Deserialize, Serialize};
use crate::common::error::{ArangoError, Result};

/// Index type matching ArangoDB's comprehensive index types
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum IndexType {
    Primary,
    Hash,
    Skiplist,
    Persistent,
    Geo,
    Geo1,
    Geo2,
    Fulltext,
    Ttl,
    Edge,
    IResearch,
    Zkd,
    Mdi,
    MdiPrefixed,
    Inverted,
    Vector,
    NoAccess,
}

/// Index definition with comprehensive options
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexDefinition {
    pub id: String,
    pub name: String,
    pub index_type: IndexType,
    pub fields: Vec<String>,
    pub unique: bool,
    pub sparse: bool,
    pub deduplicate: Option<bool>,
    pub estimates: Option<bool>,
    pub min_length: Option<u32>,
    pub geo_json: Option<bool>,
    pub constraint: Option<bool>,
    pub expansion_limit: Option<u32>,
    pub stored_values: Option<Vec<String>>,
    // Vector index specific
    pub vector_dimensions: Option<u32>,
    pub vector_similarity: Option<String>,
    // Fulltext specific
    pub min_word_length: Option<u32>,
    // TTL specific
    pub expire_after: Option<u64>,
    // Inverted index specific
    pub inverted_index_analyzer: Option<String>,
    pub inverted_index_cache: Option<bool>,
}

/// Index statistics with detailed metrics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexStatistics {
    pub name: String,
    pub size: u64,
    pub memory_usage: u64,
    pub selectivity: f64,
    pub usage_count: u64,
    pub last_used: Option<u64>,
    pub estimated_items: u64,
    pub estimated_cost: f64,
    pub cache_hit_rate: Option<f64>,
    pub index_efficiency: f64,
}

/// Filter costs calculation for query optimization
#[derive(Debug, Clone)]
pub struct FilterCosts {
    pub supports_condition: bool,
    pub covered_attributes: usize,
    pub estimated_items: u64,
    pub estimated_costs: f64,
    pub selectivity_factor: f64,
}

impl FilterCosts {
    pub fn zero_costs() -> Self {
        FilterCosts {
            supports_condition: false,
            covered_attributes: 0,
            estimated_items: 0,
            estimated_costs: 0.0,
            selectivity_factor: 1.0,
        }
    }

    pub fn default_costs(items_in_index: u64, num_lookups: usize) -> Self {
        FilterCosts {
            supports_condition: true,
            covered_attributes: 1,
            estimated_items: items_in_index / num_lookups as u64,
            estimated_costs: (items_in_index as f64).log2() * num_lookups as f64,
            selectivity_factor: 1.0 / num_lookups as f64,
        }
    }
}

/// Sort costs calculation for query optimization
#[derive(Debug, Clone)]
pub struct SortCosts {
    pub supports_condition: bool,
    pub covered_attributes: usize,
    pub estimated_costs: f64,
}

impl SortCosts {
    pub fn zero_costs(covered_attributes: usize) -> Self {
        SortCosts {
            supports_condition: false,
            covered_attributes,
            estimated_costs: 0.0,
        }
    }

    pub fn default_costs(items_in_index: u64) -> Self {
        SortCosts {
            supports_condition: true,
            covered_attributes: 1,
            estimated_costs: items_in_index as f64,
        }
    }
}

/// RocksDB index implementation with advanced features
pub struct RocksDBIndex {
    definition: IndexDefinition,
    statistics: Arc<RwLock<IndexStatistics>>,
    data_store: HashMap<Vec<u8>, Vec<Vec<u8>>>, // key -> document references
    sorted_keys: Vec<Vec<u8>>, // for sorted indexes
    is_sorted: bool,
    bloom_filter: Option<Vec<u64>>, // simple bloom filter for exists checks
}

impl RocksDBIndex {
    pub fn new(definition: IndexDefinition) -> Self {
        let is_sorted = matches!(definition.index_type, 
            IndexType::Skiplist | IndexType::Persistent | IndexType::Ttl);

        let statistics = IndexStatistics {
            name: definition.name.clone(),
            size: 0,
            memory_usage: 0,
            selectivity: 1.0,
            usage_count: 0,
            last_used: None,
            estimated_items: 0,
            estimated_cost: 0.0,
            cache_hit_rate: Some(0.0),
            index_efficiency: 1.0,
        };

        RocksDBIndex {
            definition,
            statistics: Arc::new(RwLock::new(statistics)),
            data_store: HashMap::new(),
            sorted_keys: Vec::new(),
            is_sorted,
            bloom_filter: Some(vec![0; 1024]), // 1024-bit bloom filter
        }
    }

    /// Insert a key-value pair into the index
    pub fn insert(&mut self, key: Vec<u8>, document_ref: Vec<u8>) -> Result<()> {
        // Update bloom filter
        if let Some(ref mut bloom) = self.bloom_filter {
            let hash = self.simple_hash(&key);
            let bucket = (hash % bloom.len() as u64) as usize;
            bloom[bucket] |= 1 << (hash % 64);
        }

        // Insert into data store
        self.data_store.entry(key.clone()).or_insert_with(Vec::new).push(document_ref);

        // Maintain sorted order for sorted indexes
        if self.is_sorted && !self.sorted_keys.contains(&key) {
            self.sorted_keys.push(key);
            self.sorted_keys.sort();
        }

        // Update statistics
        if let Ok(mut stats) = self.statistics.write() {
            stats.size += 1;
            stats.estimated_items += 1;
            stats.memory_usage += key.len() as u64 + 8; // rough estimate
            self.update_selectivity(&mut stats);
        }

        Ok(())
    }

    /// Calculate filter costs for query optimization
    pub fn calculate_filter_costs(&self, num_lookups: usize, condition_complexity: f64) -> FilterCosts {
        let stats = self.statistics.read().unwrap();
        let base_cost = match self.definition.index_type {
            IndexType::Hash => 1.0, // O(1) lookup
            IndexType::Skiplist | IndexType::Persistent => (stats.estimated_items as f64).log2(), // O(log n)
            IndexType::Fulltext => stats.estimated_items as f64 * 0.1, // depends on text complexity
            IndexType::Geo | IndexType::Geo1 | IndexType::Geo2 => (stats.estimated_items as f64).sqrt(), // spatial index
            _ => stats.estimated_items as f64 * 0.5, // default linear scan cost
        };

        FilterCosts {
            supports_condition: true,
            covered_attributes: self.definition.fields.len(),
            estimated_items: stats.estimated_items / num_lookups as u64,
            estimated_costs: base_cost * num_lookups as f64 * condition_complexity,
            selectivity_factor: stats.selectivity,
        }
    }

    /// Check if index supports the given condition
    pub fn supports_condition(&self, fields: &[String], operation: &str) -> bool {
        let index_fields = &self.definition.fields;

        match operation {
            "eq" | "in" => {
                !fields.is_empty() && index_fields.get(0) == fields.get(0)
            },
            "range" | "gt" | "gte" | "lt" | "lte" => {
                self.is_sorted && !fields.is_empty() && index_fields.get(0) == fields.get(0)
            },
            "text" => {
                matches!(self.definition.index_type, IndexType::Fulltext)
            },
            "geo" => {
                matches!(self.definition.index_type, IndexType::Geo | IndexType::Geo1 | IndexType::Geo2)
            },
            _ => false,
        }
    }

    /// Update selectivity estimation
    fn update_selectivity(&self, stats: &mut IndexStatistics) {
        if stats.estimated_items > 0 {
            let unique_keys = self.data_store.len() as f64;
            stats.selectivity = unique_keys / stats.estimated_items as f64;
            stats.selectivity = stats.selectivity.min(1.0).max(0.001);
        }
    }

    /// Simple hash function for bloom filter
    fn simple_hash(&self, key: &[u8]) -> u64 {
        let mut hash = 5381u64;
        for &byte in key {
            hash = hash.wrapping_mul(33).wrapping_add(byte as u64);
        }
        hash
    }

    pub fn definition(&self) -> &IndexDefinition {
        &self.definition
    }

    pub fn statistics(&self) -> &IndexStatistics {
        &self.statistics.read().unwrap()
    }
}

/// Index manager with query optimization
pub struct IndexManager {
    indexes: HashMap<String, RocksDBIndex>,
}

impl IndexManager {
    pub fn new() -> Self {
        IndexManager {
            indexes: HashMap::new(),
        }
    }

    pub fn create_index(&mut self, definition: IndexDefinition) -> Result<()> {
        let index = RocksDBIndex::new(definition.clone());
        self.indexes.insert(definition.name.clone(), index);
        Ok(())
    }

    pub fn drop_index(&mut self, name: &str) -> Result<()> {
        self.indexes.remove(name);
        Ok(())
    }

    pub fn get_index(&self, name: &str) -> Option<&RocksDBIndex> {
        self.indexes.get(name)
    }

    pub fn list_indexes(&self) -> Vec<&IndexDefinition> {
        self.indexes.values().map(|idx| idx.definition()).collect()
    }
} 