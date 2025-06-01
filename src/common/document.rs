use std::collections::HashMap;
use std::fmt;
use std::str::FromStr;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize, Serializer, Deserializer};
use serde_json::{Value, Map};
use uuid::Uuid;
use crate::common::error::{ArangoError, Result};

/// Document key type - must be a valid string identifier
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct DocumentKey(String);

impl DocumentKey {
    /// Create a new document key
    pub fn new(key: impl Into<String>) -> Result<Self> {
        let key = key.into();
        Self::validate_key(&key)?;
        Ok(DocumentKey(key))
    }
    
    /// Generate a new random document key
    pub fn generate() -> Self {
        DocumentKey(Uuid::new_v4().to_string().replace('-', ""))
    }
    
    /// Get the key as a string
    pub fn as_str(&self) -> &str {
        &self.0
    }
    
    /// Validate document key format
    fn validate_key(key: &str) -> Result<()> {
        if key.is_empty() {
            return Err(ArangoError::bad_parameter("document key cannot be empty"));
        }
        
        if key.len() > 254 {
            return Err(ArangoError::bad_parameter("document key too long (max 254 characters)"));
        }
        
        // Check if key contains only valid characters
        for c in key.chars() {
            if !c.is_alphanumeric() && c != '_' && c != '-' && c != '.' && c != ':' {
                return Err(ArangoError::bad_parameter(
                    format!("invalid character '{}' in document key", c)
                ));
            }
        }
        
        // Keys cannot start with underscore (reserved for system documents)
        if key.starts_with('_') {
            return Err(ArangoError::bad_parameter("document key cannot start with underscore"));
        }
        
        Ok(())
    }
}

impl FromStr for DocumentKey {
    type Err = ArangoError;
    
    fn from_str(s: &str) -> Result<Self> {
        DocumentKey::new(s)
    }
}

impl fmt::Display for DocumentKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<DocumentKey> for String {
    fn from(key: DocumentKey) -> String {
        key.0
    }
}

/// Document ID - combination of collection name and document key
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct DocumentId {
    pub collection: String,
    pub key: DocumentKey,
}

impl DocumentId {
    /// Create a new document ID
    pub fn new(collection: impl Into<String>, key: DocumentKey) -> Self {
        DocumentId {
            collection: collection.into(),
            key,
        }
    }
    
    /// Parse document ID from string format "collection/key"
    pub fn parse(id: &str) -> Result<Self> {
        let parts: Vec<&str> = id.split('/').collect();
        if parts.len() != 2 {
            return Err(ArangoError::bad_parameter(
                "document ID must be in format 'collection/key'"
            ));
        }
        
        let collection = parts[0].to_string();
        let key = DocumentKey::new(parts[1])?;
        
        Ok(DocumentId { collection, key })
    }
    
    /// Get collection name
    pub fn collection(&self) -> &str {
        &self.collection
    }
    
    /// Get document key
    pub fn key(&self) -> &DocumentKey {
        &self.key
    }
    
    /// Convert to string format "collection/key"
    pub fn to_string(&self) -> String {
        format!("{}/{}", self.collection, self.key)
    }
}

impl fmt::Display for DocumentId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}/{}", self.collection, self.key)
    }
}

impl Serialize for DocumentId {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> Deserialize<'de> for DocumentId {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        DocumentId::parse(&s).map_err(serde::de::Error::custom)
    }
}

/// Document revision - used for optimistic locking
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, PartialOrd, Ord)]
pub struct DocumentRevision(String);

impl DocumentRevision {
    /// Create a new revision
    pub fn new() -> Self {
        DocumentRevision(format!("{}", chrono::Utc::now().timestamp_nanos()))
    }
    
    /// Create revision from string
    pub fn from_string(rev: String) -> Self {
        DocumentRevision(rev)
    }
    
    /// Get revision as string
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Default for DocumentRevision {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for DocumentRevision {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Document metadata
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct DocumentMetadata {
    /// Document ID
    #[serde(rename = "_id")]
    pub id: DocumentId,
    
    /// Document key
    #[serde(rename = "_key")]
    pub key: DocumentKey,
    
    /// Document revision
    #[serde(rename = "_rev")]
    pub revision: DocumentRevision,
    
    /// Creation timestamp
    #[serde(rename = "_created")]
    pub created: DateTime<Utc>,
    
    /// Last modification timestamp
    #[serde(rename = "_modified")]
    pub modified: DateTime<Utc>,
}

impl PartialOrd for DocumentMetadata {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for DocumentMetadata {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.id.cmp(&other.id)
            .then_with(|| self.revision.cmp(&other.revision))
    }
}

impl DocumentMetadata {
    /// Create new metadata
    pub fn new(id: DocumentId) -> Self {
        let now = Utc::now();
        let key = id.key().clone();
        
        DocumentMetadata {
            id,
            key,
            revision: DocumentRevision::new(),
            created: now,
            modified: now,
        }
    }
    
    /// Update revision and modification time
    pub fn update_revision(&mut self) {
        self.revision = DocumentRevision::new();
        self.modified = Utc::now();
    }
}

/// Main document type
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct Document {
    /// Document metadata (flattened into the document)
    #[serde(flatten)]
    pub metadata: DocumentMetadata,
    
    /// Document data as a JSON object
    #[serde(flatten)]
    pub data: Map<String, Value>,
}

impl PartialOrd for Document {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Document {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.metadata.cmp(&other.metadata)
    }
}

impl Document {
    /// Create a new document
    pub fn new(collection: impl Into<String>, data: Map<String, Value>) -> Result<Self> {
        // Extract key from data if present, otherwise generate one
        let key = if let Some(key_value) = data.get("_key") {
            if let Some(key_str) = key_value.as_str() {
                DocumentKey::new(key_str)?
            } else {
                return Err(ArangoError::bad_parameter("_key must be a string"));
            }
        } else {
            DocumentKey::generate()
        };
        
        let id = DocumentId::new(collection, key);
        let metadata = DocumentMetadata::new(id);
        
        // Remove system fields from data
        let mut clean_data = data;
        clean_data.remove("_key");
        clean_data.remove("_id");
        clean_data.remove("_rev");
        clean_data.remove("_created");
        clean_data.remove("_modified");
        
        Ok(Document {
            metadata,
            data: clean_data,
        })
    }
    
    /// Create document with specific key
    pub fn with_key(collection: impl Into<String>, key: DocumentKey, data: Map<String, Value>) -> Self {
        let id = DocumentId::new(collection, key);
        let metadata = DocumentMetadata::new(id);
        
        // Remove system fields from data
        let mut clean_data = data;
        clean_data.remove("_key");
        clean_data.remove("_id");
        clean_data.remove("_rev");
        clean_data.remove("_created");
        clean_data.remove("_modified");
        
        Document {
            metadata,
            data: clean_data,
        }
    }
    
    /// Get document ID
    pub fn id(&self) -> &DocumentId {
        &self.metadata.id
    }
    
    /// Get document key
    pub fn key(&self) -> &DocumentKey {
        &self.metadata.key
    }
    
    /// Get document revision
    pub fn revision(&self) -> &DocumentRevision {
        &self.metadata.revision
    }
    
    /// Get collection name
    pub fn collection(&self) -> &str {
        self.metadata.id.collection()
    }
    
    /// Get a field value
    pub fn get(&self, field: &str) -> Option<&Value> {
        self.data.get(field)
    }
    
    /// Set a field value
    pub fn set(&mut self, field: String, value: Value) {
        self.data.insert(field, value);
        self.metadata.update_revision();
    }
    
    /// Remove a field
    pub fn remove(&mut self, field: &str) -> Option<Value> {
        let result = self.data.remove(field);
        if result.is_some() {
            self.metadata.update_revision();
        }
        result
    }
    
    /// Get all field names
    pub fn fields(&self) -> Vec<&String> {
        self.data.keys().collect()
    }
    
    /// Convert to JSON value (including metadata)
    pub fn to_json(&self) -> Value {
        serde_json::to_value(self).unwrap_or(Value::Null)
    }
    
    /// Create from JSON value
    pub fn from_json(collection: impl Into<String>, value: Value) -> Result<Self> {
        if let Value::Object(map) = value {
            Document::new(collection, map)
        } else {
            Err(ArangoError::bad_parameter("document must be a JSON object"))
        }
    }
    
    /// Update document data while preserving metadata
    pub fn update(&mut self, data: Map<String, Value>) {
        // Remove system fields from new data
        let mut clean_data = data;
        clean_data.remove("_key");
        clean_data.remove("_id");
        clean_data.remove("_rev");
        clean_data.remove("_created");
        clean_data.remove("_modified");
        
        self.data = clean_data;
        self.metadata.update_revision();
    }
    
    /// Merge new data into existing document
    pub fn merge(&mut self, data: Map<String, Value>) {
        for (key, value) in data {
            // Skip system fields
            if !key.starts_with('_') {
                self.data.insert(key, value);
            }
        }
        self.metadata.update_revision();
    }
    
    /// Get document as a map without metadata
    pub fn data_only(&self) -> Map<String, Value> {
        self.data.clone()
    }
    
    /// Get document size in bytes (approximate)
    pub fn size(&self) -> usize {
        serde_json::to_string(self).map(|s| s.len()).unwrap_or(0)
    }
    
    /// Check if document is empty (no user data)
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }
}

/// Edge document - extends regular document with edge-specific fields
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct EdgeDocument {
    /// Base document
    #[serde(flatten)]
    pub document: Document,
    
    /// Source vertex ID
    #[serde(rename = "_from")]
    pub from: DocumentId,
    
    /// Target vertex ID
    #[serde(rename = "_to")]
    pub to: DocumentId,
}

impl PartialOrd for EdgeDocument {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for EdgeDocument {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.document.cmp(&other.document)
            .then_with(|| self.from.cmp(&other.from))
            .then_with(|| self.to.cmp(&other.to))
    }
}

impl EdgeDocument {
    /// Create a new edge document
    pub fn new(
        collection: impl Into<String>,
        from: DocumentId,
        to: DocumentId,
        data: Map<String, Value>,
    ) -> Result<Self> {
        let mut document = Document::new(collection, data)?;
        
        // Remove edge fields from document data if present
        document.data.remove("_from");
        document.data.remove("_to");
        
        Ok(EdgeDocument {
            document,
            from,
            to,
        })
    }
    
    /// Get source vertex ID
    pub fn from(&self) -> &DocumentId {
        &self.from
    }
    
    /// Get target vertex ID
    pub fn to(&self) -> &DocumentId {
        &self.to
    }
    
    /// Get the underlying document
    pub fn document(&self) -> &Document {
        &self.document
    }
    
    /// Get mutable reference to underlying document
    pub fn document_mut(&mut self) -> &mut Document {
        &mut self.document
    }
    
    /// Convert to JSON value (including edge metadata)
    pub fn to_json(&self) -> Value {
        serde_json::to_value(self).unwrap_or(Value::Null)
    }
}

/// Document operation result
#[derive(Debug, Clone)]
pub struct DocumentOperationResult {
    pub id: DocumentId,
    pub key: DocumentKey,
    pub revision: DocumentRevision,
    pub old_revision: Option<DocumentRevision>,
}

impl DocumentOperationResult {
    pub fn new(document: &Document) -> Self {
        DocumentOperationResult {
            id: document.id().clone(),
            key: document.key().clone(),
            revision: document.revision().clone(),
            old_revision: None,
        }
    }
    
    pub fn with_old_revision(mut self, old_rev: DocumentRevision) -> Self {
        self.old_revision = Some(old_rev);
        self
    }
}

/// Document query filter
#[derive(Debug, Clone)]
pub enum DocumentFilter {
    /// Match by key
    Key(DocumentKey),
    /// Match by ID
    Id(DocumentId),
    /// Match by field value
    Field {
        field: String,
        value: Value,
    },
    /// Match by multiple conditions (AND)
    And(Vec<DocumentFilter>),
    /// Match by any condition (OR)
    Or(Vec<DocumentFilter>),
    /// Match all documents
    All,
}

impl DocumentFilter {
    /// Check if a document matches this filter
    pub fn matches(&self, document: &Document) -> bool {
        match self {
            DocumentFilter::Key(key) => document.key() == key,
            DocumentFilter::Id(id) => document.id() == id,
            DocumentFilter::Field { field, value } => {
                document.get(field).map_or(false, |v| v == value)
            }
            DocumentFilter::And(filters) => {
                filters.iter().all(|f| f.matches(document))
            }
            DocumentFilter::Or(filters) => {
                filters.iter().any(|f| f.matches(document))
            }
            DocumentFilter::All => true,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_document_key_validation() {
        assert!(DocumentKey::new("valid_key123").is_ok());
        assert!(DocumentKey::new("").is_err());
        assert!(DocumentKey::new("_system").is_err());
        assert!(DocumentKey::new("key with spaces").is_err());
        assert!(DocumentKey::new("a".repeat(255)).is_err());
    }

    #[test]
    fn test_document_creation() {
        let mut data = Map::new();
        data.insert("name".to_string(), json!("test"));
        data.insert("value".to_string(), json!(42));
        
        let doc = Document::new("test_collection", data).unwrap();
        assert_eq!(doc.collection(), "test_collection");
        assert_eq!(doc.get("name"), Some(&json!("test")));
        assert_eq!(doc.get("value"), Some(&json!(42)));
    }

    #[test]
    fn test_document_id_parsing() {
        let id = DocumentId::parse("collection/key123").unwrap();
        assert_eq!(id.collection(), "collection");
        assert_eq!(id.key().as_str(), "key123");
        assert_eq!(id.to_string(), "collection/key123");
    }

    #[test]
    fn test_edge_document() {
        let from = DocumentId::parse("vertices/v1").unwrap();
        let to = DocumentId::parse("vertices/v2").unwrap();
        
        let mut data = Map::new();
        data.insert("weight".to_string(), json!(1.0));
        
        let edge = EdgeDocument::new("edges", from.clone(), to.clone(), data).unwrap();
        assert_eq!(edge.from(), &from);
        assert_eq!(edge.to(), &to);
        assert_eq!(edge.document().get("weight"), Some(&json!(1.0)));
    }
} 