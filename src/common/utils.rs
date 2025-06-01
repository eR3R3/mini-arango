use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};
use sha2::{Sha256, Digest};
use regex::Regex;
use crate::common::error::{ArangoError, Result};
use std::fmt;
use serde::{Deserialize, Serialize};
use chrono::{DateTime, Utc};
use crate::common::document::DocumentId;

/// Generate a unique ID based on timestamp and random data
pub fn generate_id() -> String {
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let random = uuid::Uuid::new_v4().to_string().replace('-', "");
    format!("{}{}", timestamp, &random[..8])
}

/// Generate a SHA256 hash of the input data
pub fn sha256_hash(data: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(data);
    hex::encode(hasher.finalize())
}

/// Generate MD5 hash (for compatibility with ArangoDB)
pub fn md5_hash(data: &[u8]) -> String {
    let digest = md5::compute(data);
    format!("{:x}", digest)
}

/// Validate collection name according to ArangoDB rules
pub fn validate_collection_name(name: &str) -> Result<()> {
    if name.is_empty() {
        return Err(ArangoError::bad_parameter("collection name cannot be empty"));
    }
    
    if name.len() > 256 {
        return Err(ArangoError::bad_parameter("collection name too long (max 256 characters)"));
    }
    
    // Must start with letter or underscore
    let first_char = name.chars().next().unwrap();
    if !first_char.is_alphabetic() && first_char != '_' {
        return Err(ArangoError::bad_parameter(
            "collection name must start with a letter or underscore"
        ));
    }
    
    // Only allow alphanumeric, underscore, and hyphen
    for c in name.chars() {
        if !c.is_alphanumeric() && c != '_' && c != '-' {
            return Err(ArangoError::bad_parameter(
                format!("invalid character '{}' in collection name", c)
            ));
        }
    }
    
    Ok(())
}

/// Validate database name according to ArangoDB rules
pub fn validate_database_name(name: &str) -> Result<()> {
    if name.is_empty() {
        return Err(ArangoError::bad_parameter("database name cannot be empty"));
    }
    
    if name.len() > 64 {
        return Err(ArangoError::bad_parameter("database name too long (max 64 characters)"));
    }
    
    // Must start with letter
    let first_char = name.chars().next().unwrap();
    if !first_char.is_alphabetic() {
        return Err(ArangoError::bad_parameter(
            "database name must start with a letter"
        ));
    }
    
    // Only allow alphanumeric, underscore, and hyphen
    for c in name.chars() {
        if !c.is_alphanumeric() && c != '_' && c != '-' {
            return Err(ArangoError::bad_parameter(
                format!("invalid character '{}' in database name", c)
            ));
        }
    }
    
    // Reserved names
    let reserved = ["_system", "system"];
    if reserved.contains(&name) && name != "_system" {
        return Err(ArangoError::bad_parameter(
            format!("database name '{}' is reserved", name)
        ));
    }
    
    Ok(())
}

/// URL-encode a string
pub fn url_encode(input: &str) -> String {
    url::form_urlencoded::byte_serialize(input.as_bytes()).collect()
}

/// URL-decode a string
pub fn url_decode(input: &str) -> Result<String> {
    url::form_urlencoded::parse(input.as_bytes())
        .map(|(key, _)| key.into_owned())
        .next()
        .ok_or_else(|| ArangoError::bad_parameter("invalid URL encoding"))
}

/// Convert bytes to human-readable format
pub fn format_bytes(bytes: u64) -> String {
    const UNITS: &[&str] = &["B", "KB", "MB", "GB", "TB", "PB"];
    const THRESHOLD: u64 = 1024;
    
    if bytes < THRESHOLD {
        return format!("{} B", bytes);
    }
    
    let mut size = bytes as f64;
    let mut unit_index = 0;
    
    while size >= THRESHOLD as f64 && unit_index < UNITS.len() - 1 {
        size /= THRESHOLD as f64;
        unit_index += 1;
    }
    
    format!("{:.1} {}", size, UNITS[unit_index])
}

/// Parse human-readable byte format back to bytes
pub fn parse_bytes(input: &str) -> Result<u64> {
    let input = input.trim().to_uppercase();
    
    let re = Regex::new(r"^(\d+(?:\.\d+)?)\s*([KMGTPB]*B?)$").unwrap();
    let caps = re.captures(&input)
        .ok_or_else(|| ArangoError::bad_parameter("invalid byte format"))?;
    
    let number: f64 = caps[1].parse()
        .map_err(|_| ArangoError::bad_parameter("invalid number in byte format"))?;
    
    let unit = caps.get(2).map_or("B", |m| m.as_str());
    
    let multiplier = match unit {
        "B" | "" => 1,
        "KB" => 1024,
        "MB" => 1024_u64.pow(2),
        "GB" => 1024_u64.pow(3),
        "TB" => 1024_u64.pow(4),
        "PB" => 1024_u64.pow(5),
        _ => return Err(ArangoError::bad_parameter("unknown byte unit")),
    };
    
    Ok((number * multiplier as f64) as u64)
}

/// Sanitize a string for safe use in file paths
pub fn sanitize_filename(input: &str) -> String {
    let mut result = String::new();
    
    for c in input.chars() {
        match c {
            'a'..='z' | 'A'..='Z' | '0'..='9' | '_' | '-' | '.' => result.push(c),
            ' ' => result.push('_'),
            _ => result.push_str(&format!("_{:x}_", c as u32)),
        }
    }
    
    // Ensure it doesn't start with a dot
    if result.starts_with('.') {
        result = format!("_{}", result);
    }
    
    result
}

/// Create a directory path if it doesn't exist
pub fn ensure_directory(path: &Path) -> Result<()> {
    if !path.exists() {
        std::fs::create_dir_all(path)
            .map_err(|e| ArangoError::internal(format!("failed to create directory: {}", e)))?;
    } else if !path.is_dir() {
        return Err(ArangoError::internal(format!("{} exists but is not a directory", path.display())));
    }
    
    Ok(())
}

/// Get file size
pub fn file_size(path: &Path) -> Result<u64> {
    let metadata = std::fs::metadata(path)
        .map_err(|e| ArangoError::internal(format!("failed to get file metadata: {}", e)))?;
    Ok(metadata.len())
}

/// Check if a string is a valid JSON
pub fn is_valid_json(input: &str) -> bool {
    serde_json::from_str::<serde_json::Value>(input).is_ok()
}

/// Pretty-print JSON
pub fn pretty_json(value: &serde_json::Value) -> Result<String> {
    serde_json::to_string_pretty(value)
        .map_err(|e| ArangoError::internal(format!("failed to format JSON: {}", e)))
}

/// Compact JSON (remove whitespace)
pub fn compact_json(value: &serde_json::Value) -> Result<String> {
    serde_json::to_string(value)
        .map_err(|e| ArangoError::internal(format!("failed to serialize JSON: {}", e)))
}

/// Generate a random string of specified length
pub fn random_string(length: usize) -> String {
    use rand::{distributions::Alphanumeric, Rng};
    
    rand::thread_rng()
        .sample_iter(&Alphanumeric)
        .take(length)
        .map(char::from)
        .collect()
}

/// Calculate elapsed time in milliseconds
pub fn elapsed_ms(start: std::time::Instant) -> u64 {
    start.elapsed().as_millis() as u64
}

/// Parse duration from string (e.g., "1s", "500ms", "2m", "1h")
pub fn parse_duration(input: &str) -> Result<std::time::Duration> {
    let re = Regex::new(r"^(\d+(?:\.\d+)?)(ms|s|m|h|d)$").unwrap();
    let caps = re.captures(input.trim())
        .ok_or_else(|| ArangoError::bad_parameter("invalid duration format"))?;
    
    let number: f64 = caps[1].parse()
        .map_err(|_| ArangoError::bad_parameter("invalid number in duration"))?;
    
    let unit = &caps[2];
    
    let duration = match unit {
        "ms" => std::time::Duration::from_millis(number as u64),
        "s" => std::time::Duration::from_secs_f64(number),
        "m" => std::time::Duration::from_secs_f64(number * 60.0),
        "h" => std::time::Duration::from_secs_f64(number * 3600.0),
        "d" => std::time::Duration::from_secs_f64(number * 86400.0),
        _ => return Err(ArangoError::bad_parameter("unknown duration unit")),
    };
    
    Ok(duration)
}

/// Format duration as human-readable string
pub fn format_duration(duration: std::time::Duration) -> String {
    let total_secs = duration.as_secs_f64();
    
    if total_secs < 1.0 {
        format!("{}ms", duration.as_millis())
    } else if total_secs < 60.0 {
        format!("{:.1}s", total_secs)
    } else if total_secs < 3600.0 {
        format!("{:.1}m", total_secs / 60.0)
    } else if total_secs < 86400.0 {
        format!("{:.1}h", total_secs / 3600.0)
    } else {
        format!("{:.1}d", total_secs / 86400.0)
    }
}

/// Merge two JSON objects
pub fn merge_json_objects(
    mut base: serde_json::Map<String, serde_json::Value>,
    update: serde_json::Map<String, serde_json::Value>,
) -> serde_json::Map<String, serde_json::Value> {
    for (key, value) in update {
        base.insert(key, value);
    }
    base
}

/// Deep merge two JSON objects (recursively merge nested objects)
pub fn deep_merge_json_objects(
    mut base: serde_json::Map<String, serde_json::Value>,
    update: serde_json::Map<String, serde_json::Value>,
) -> serde_json::Map<String, serde_json::Value> {
    for (key, new_value) in update {
        match (base.get(&key), &new_value) {
            (Some(serde_json::Value::Object(base_obj)), serde_json::Value::Object(new_obj)) => {
                // Both are objects, merge recursively
                let merged = deep_merge_json_objects(base_obj.clone(), new_obj.clone());
                base.insert(key, serde_json::Value::Object(merged));
            }
            _ => {
                // Either not both objects, or key doesn't exist in base
                base.insert(key, new_value);
            }
        }
    }
    base
}

/// Extract fields from a JSON object based on field list
pub fn extract_fields(
    object: &serde_json::Map<String, serde_json::Value>,
    fields: &[String],
) -> serde_json::Map<String, serde_json::Value> {
    let mut result = serde_json::Map::new();
    
    for field in fields {
        if let Some(value) = object.get(field) {
            result.insert(field.clone(), value.clone());
        }
    }
    
    result
}

/// Check if a value matches a pattern (simple glob-style matching)
pub fn matches_pattern(value: &str, pattern: &str) -> bool {
    if pattern == "*" {
        return true;
    }
    
    if !pattern.contains('*') && !pattern.contains('?') {
        return value == pattern;
    }
    
    // Convert glob pattern to regex
    let regex_pattern = pattern
        .replace(".", r"\.")
        .replace("*", ".*")
        .replace("?", ".");
    
    if let Ok(re) = Regex::new(&format!("^{}$", regex_pattern)) {
        re.is_match(value)
    } else {
        false
    }
}

/// Retry a function with exponential backoff
pub async fn retry_with_backoff<F, Fut, T, E>(
    mut operation: F,
    max_attempts: usize,
    initial_delay: std::time::Duration,
) -> std::result::Result<T, E>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = std::result::Result<T, E>>,
{
    let mut delay = initial_delay;
    
    for attempt in 1..=max_attempts {
        match operation().await {
            Ok(result) => return Ok(result),
            Err(error) => {
                if attempt == max_attempts {
                    return Err(error);
                }
                tokio::time::sleep(delay).await;
                delay *= 2; // Exponential backoff
            }
        }
    }
    
    unreachable!()
}

/// Convert system timestamp to ISO8601 string
pub fn timestamp_to_iso8601(timestamp: i64) -> String {
    if let Some(datetime) = chrono::DateTime::from_timestamp(timestamp, 0) {
        datetime.to_rfc3339()
    } else {
        chrono::Utc::now().to_rfc3339()
    }
}

/// Parse ISO8601 string to timestamp
pub fn iso8601_to_timestamp(iso_string: &str) -> Result<i64> {
    chrono::DateTime::parse_from_rfc3339(iso_string)
        .map(|dt| dt.timestamp())
        .map_err(|_| ArangoError::bad_parameter("invalid ISO8601 timestamp"))
}

/// Document metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DocumentMetadata {
    pub id: DocumentId,
    pub revision: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: Option<DateTime<Utc>>,
    pub collection: String,
}

impl DocumentMetadata {
    pub fn new(id: DocumentId, revision: String, collection: String) -> Self {
        let now = Utc::now();
        DocumentMetadata {
            id,
            revision,
            created_at: now,
            updated_at: None,
            collection,
        }
    }
    
    pub fn mark_updated(&mut self) {
        self.updated_at = Some(Utc::now());
    }
}

/// Parse a timestamp string into a DateTime
pub fn parse_timestamp(timestamp_str: &str) -> Result<DateTime<Utc>> {
    match DateTime::parse_from_rfc3339(timestamp_str) {
        Ok(dt) => Ok(dt.with_timezone(&Utc)),
        Err(_) => {
            // Try parsing as Unix timestamp
            if let Ok(timestamp) = timestamp_str.parse::<i64>() {
                if let Some(dt) = chrono::DateTime::from_timestamp(timestamp, 0) {
                    Ok(dt)
                } else {
                    Err(crate::common::error::ArangoError::bad_parameter("Invalid timestamp"))
                }
            } else {
                Err(crate::common::error::ArangoError::bad_parameter("Invalid timestamp format"))
            }
        }
    }
}

/// Format a DateTime as a string
pub fn format_timestamp(dt: &DateTime<Utc>) -> String {
    dt.to_rfc3339()
}

/// Generate a unique revision ID
pub fn generate_revision_id() -> String {
    match Utc::now().timestamp_nanos_opt() {
        Some(nanos) => format!("{}", nanos),
        None => format!("{}", Utc::now().timestamp_millis()),
    }
}

/// Validate a document key
pub fn validate_document_key(key: &str) -> Result<()> {
    if key.is_empty() {
        return Err(crate::common::error::ArangoError::bad_parameter("Document key cannot be empty"));
    }
    
    if key.len() > 254 {
        return Err(crate::common::error::ArangoError::bad_parameter("Document key too long"));
    }
    
    // Check for valid characters
    if !key.chars().all(|c| c.is_alphanumeric() || c == '_' || c == '-' || c == '.') {
        return Err(crate::common::error::ArangoError::bad_parameter(
            "Document key contains invalid characters"
        ));
    }
    
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use crate::DocumentKey;

    #[test]
    fn test_validate_collection_name() {
        assert!(validate_collection_name("valid_collection").is_ok());
        assert!(validate_collection_name("_system").is_ok());
        assert!(validate_collection_name("collection-123").is_ok());
        assert!(validate_collection_name("").is_err());
        assert!(validate_collection_name("123invalid").is_err());
        assert!(validate_collection_name("invalid@name").is_err());
    }

    #[test]
    fn test_validate_database_name() {
        assert!(validate_database_name("mydb").is_ok());
        assert!(validate_database_name("my_db").is_ok());
        assert!(validate_database_name("my-db").is_ok());
        assert!(validate_database_name("").is_err());
        assert!(validate_database_name("123invalid").is_err());
        assert!(validate_database_name("invalid@name").is_err());
    }

    #[test]
    fn test_format_bytes() {
        assert_eq!(format_bytes(1024), "1.0 KB");
        assert_eq!(format_bytes(1536), "1.5 KB");
        assert_eq!(format_bytes(1048576), "1.0 MB");
        assert_eq!(format_bytes(500), "500 B");
    }

    #[test]
    fn test_parse_bytes() {
        assert_eq!(parse_bytes("1024 B").unwrap(), 1024);
        assert_eq!(parse_bytes("1 KB").unwrap(), 1024);
        assert_eq!(parse_bytes("1.5 MB").unwrap(), 1572864);
        assert!(parse_bytes("invalid").is_err());
    }

    #[test]
    fn test_matches_pattern() {
        assert!(matches_pattern("hello", "*"));
        assert!(matches_pattern("hello", "hello"));
        assert!(matches_pattern("hello", "h*"));
        assert!(matches_pattern("hello", "h?llo"));
        assert!(!matches_pattern("hello", "world"));
    }

    #[test]
    fn test_merge_json_objects() {
        let base = json!({"a": 1, "b": 2}).as_object().unwrap().clone();
        let update = json!({"b": 3, "c": 4}).as_object().unwrap().clone();
        
        let merged = merge_json_objects(base, update);
        assert_eq!(merged.get("a"), Some(&json!(1)));
        assert_eq!(merged.get("b"), Some(&json!(3)));
        assert_eq!(merged.get("c"), Some(&json!(4)));
    }

    #[test]
    fn test_document_metadata() {
        let id = DocumentId::new("test_doc", DocumentKey::generate());
        let mut meta = DocumentMetadata::new(id, "1234".to_string(), "test_collection".to_string());
        
        assert!(meta.updated_at.is_none());
        meta.mark_updated();
        assert!(meta.updated_at.is_some());
    }

    #[test]
    fn test_timestamp_parsing() {
        let timestamp_str = "2023-01-01T00:00:00Z";
        let result = parse_timestamp(timestamp_str);
        assert!(result.is_ok());
        
        let unix_timestamp = "1672531200";
        let result = parse_timestamp(unix_timestamp);
        assert!(result.is_ok());
    }

    #[test]
    fn test_collection_name_validation() {
        assert!(validate_collection_name("valid_name").is_ok());
        assert!(validate_collection_name("_system").is_ok());
        assert!(validate_collection_name("").is_err());
        assert!(validate_collection_name("123invalid").is_err());
    }

    #[test]
    fn test_document_key_validation() {
        assert!(validate_document_key("valid_key").is_ok());
        assert!(validate_document_key("key-123").is_ok());
        assert!(validate_document_key("key.test").is_ok());
        assert!(validate_document_key("").is_err());
        assert!(validate_document_key("key with spaces").is_err());
    }
} 