use std::fmt;
use serde::{Deserialize, Serialize};

/// Tick type (56bit) - used for versioning and timestamps
pub type VocTick = u64;

/// Collection type enumeration
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[repr(u32)]
pub enum CollectionType {
    Unknown = 0,
    Document = 2,
    Edge = 3,
}

impl CollectionType {
    pub fn from_u32(value: u32) -> Self {
        match value {
            2 => CollectionType::Document,
            3 => CollectionType::Edge,
            _ => CollectionType::Unknown,
        }
    }
    
    pub fn as_u32(&self) -> u32 {
        *self as u32
    }
    
    pub fn is_edge(&self) -> bool {
        matches!(self, CollectionType::Edge)
    }
    
    pub fn is_document(&self) -> bool {
        matches!(self, CollectionType::Document)
    }
}

impl Default for CollectionType {
    fn default() -> Self {
        CollectionType::Document
    }
}

impl fmt::Display for CollectionType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CollectionType::Unknown => write!(f, "unknown"),
            CollectionType::Document => write!(f, "document"),
            CollectionType::Edge => write!(f, "edge"),
        }
    }
}

/// Document operation types
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[repr(u8)]
pub enum DocumentOperation {
    Unknown = 0,
    Insert = 1,
    Update = 2,
    Replace = 3,
    Remove = 4,
}

impl DocumentOperation {
    pub fn from_u8(value: u8) -> Self {
        match value {
            1 => DocumentOperation::Insert,
            2 => DocumentOperation::Update,
            3 => DocumentOperation::Replace,
            4 => DocumentOperation::Remove,
            _ => DocumentOperation::Unknown,
        }
    }
    
    pub fn as_u8(&self) -> u8 {
        *self as u8
    }
}

impl Default for DocumentOperation {
    fn default() -> Self {
        DocumentOperation::Unknown
    }
}

impl fmt::Display for DocumentOperation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DocumentOperation::Unknown => write!(f, "unknown"),
            DocumentOperation::Insert => write!(f, "insert"),
            DocumentOperation::Update => write!(f, "update"),
            DocumentOperation::Replace => write!(f, "replace"),
            DocumentOperation::Remove => write!(f, "remove"),
        }
    }
}

/// Edge direction for traversals
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[repr(u8)]
pub enum EdgeDirection {
    Any = 0,
    In = 1,
    Out = 2,
}

impl EdgeDirection {
    pub fn from_u8(value: u8) -> Self {
        match value {
            1 => EdgeDirection::In,
            2 => EdgeDirection::Out,
            _ => EdgeDirection::Any,
        }
    }
    
    pub fn as_u8(&self) -> u8 {
        *self as u8
    }
    
    pub fn reverse(&self) -> Self {
        match self {
            EdgeDirection::In => EdgeDirection::Out,
            EdgeDirection::Out => EdgeDirection::In,
            EdgeDirection::Any => EdgeDirection::Any,
        }
    }
}

impl Default for EdgeDirection {
    fn default() -> Self {
        EdgeDirection::Any
    }
}

impl fmt::Display for EdgeDirection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            EdgeDirection::Any => write!(f, "any"),
            EdgeDirection::In => write!(f, "in"),
            EdgeDirection::Out => write!(f, "out"),
        }
    }
}

/// Sharding prototype for collections
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[repr(u32)]
pub enum ShardingPrototype {
    Undefined = 0,
    Users = 1,
    Graphs = 2,
}

impl ShardingPrototype {
    pub fn from_u32(value: u32) -> Self {
        match value {
            1 => ShardingPrototype::Users,
            2 => ShardingPrototype::Graphs,
            _ => ShardingPrototype::Undefined,
        }
    }
    
    pub fn as_u32(&self) -> u32 {
        *self as u32
    }
}

impl Default for ShardingPrototype {
    fn default() -> Self {
        ShardingPrototype::Undefined
    }
}

/// Data source identifiers
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct DataSourceId(pub u64);

impl DataSourceId {
    pub fn new(id: u64) -> Self {
        DataSourceId(id)
    }
    
    pub fn id(&self) -> u64 {
        self.0
    }
}

impl From<u64> for DataSourceId {
    fn from(id: u64) -> Self {
        DataSourceId(id)
    }
}

impl fmt::Display for DataSourceId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Revision identifier for documents
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct RevisionId(String);

impl RevisionId {
    /// Create a new revision ID
    pub fn create() -> Self {
        RevisionId(format!("{}", chrono::Utc::now().timestamp_nanos()))
    }
    
    /// Create from string
    pub fn from_string(s: String) -> Self {
        RevisionId(s)
    }
    
    /// Get as string
    pub fn as_str(&self) -> &str {
        &self.0
    }
    
    /// Create cluster-wide unique revision (simplified for mini-arangodb)
    pub fn create_cluster_wide_unique() -> Self {
        RevisionId(format!("cluster-{}", chrono::Utc::now().timestamp_nanos()))
    }
}

impl fmt::Display for RevisionId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Index identifier
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct IndexId(pub u64);

impl IndexId {
    pub fn new(id: u64) -> Self {
        IndexId(id)
    }
    
    pub fn id(&self) -> u64 {
        self.0
    }
}

impl From<u64> for IndexId {
    fn from(id: u64) -> Self {
        IndexId(id)
    }
}

impl fmt::Display for IndexId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Shard identifier for distributed collections
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ShardId(String);

impl ShardId {
    pub fn new(id: String) -> Self {
        ShardId(id)
    }
    
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for ShardId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Local document identifier (within a collection)
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct LocalDocumentId {
    value: u64,
}

impl LocalDocumentId {
    pub fn new(value: u64) -> Self {
        LocalDocumentId { value }
    }
    
    pub fn value(&self) -> u64 {
        self.value
    }
}

impl From<u64> for LocalDocumentId {
    fn from(value: u64) -> Self {
        LocalDocumentId::new(value)
    }
}

impl fmt::Display for LocalDocumentId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.value)
    }
} 