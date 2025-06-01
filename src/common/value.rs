use std::collections::HashMap;
use std::fmt;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use crate::common::error::{ArangoError, Result};

/// ArangoDB Value types based on VelocyPack and AqlValue
/// This represents the actual data types supported by ArangoDB
#[derive(Debug, Clone, PartialEq)]
pub enum ArangoValue {
    /// No value (uninitialized)
    None,
    
    /// Null value
    Null,
    
    /// Boolean value
    Bool(bool),
    
    /// 64-bit signed integer
    Int(i64),
    
    /// 64-bit unsigned integer  
    UInt(u64),
    
    /// 64-bit floating point number
    Double(f64),
    
    /// UTF-8 string
    String(String),
    
    /// Array of values
    Array(Vec<ArangoValue>),
    
    /// Object/Document with string keys
    Object(HashMap<String, ArangoValue>),
    
    /// Binary data (bytes)
    Binary(Vec<u8>),
    
    /// Range type (used in AQL)
    Range { low: i64, high: i64 },
    
    /// Date/time as Unix timestamp in milliseconds
    DateTime(i64),
}

impl ArangoValue {
    /// Create a new None value
    pub fn none() -> Self {
        ArangoValue::None
    }
    
    /// Create a new Null value
    pub fn null() -> Self {
        ArangoValue::Null
    }
    
    /// Create a new Bool value
    pub fn bool(value: bool) -> Self {
        ArangoValue::Bool(value)
    }
    
    /// Create a new Int value
    pub fn int(value: i64) -> Self {
        ArangoValue::Int(value)
    }
    
    /// Create a new UInt value
    pub fn uint(value: u64) -> Self {
        ArangoValue::UInt(value)
    }
    
    /// Create a new Double value
    pub fn double(value: f64) -> Self {
        ArangoValue::Double(value)
    }
    
    /// Create a new String value
    pub fn string<S: Into<String>>(value: S) -> Self {
        ArangoValue::String(value.into())
    }
    
    /// Create a new Array value
    pub fn array(values: Vec<ArangoValue>) -> Self {
        ArangoValue::Array(values)
    }
    
    /// Create a new Object value
    pub fn object(map: HashMap<String, ArangoValue>) -> Self {
        ArangoValue::Object(map)
    }
    
    /// Create a new Binary value
    pub fn binary(data: Vec<u8>) -> Self {
        ArangoValue::Binary(data)
    }
    
    /// Create a new Range value
    pub fn range(low: i64, high: i64) -> Self {
        ArangoValue::Range { low, high }
    }
    
    /// Create a new DateTime value
    pub fn datetime(timestamp: i64) -> Self {
        ArangoValue::DateTime(timestamp)
    }
    
    /// Check if value is None
    pub fn is_none(&self) -> bool {
        matches!(self, ArangoValue::None)
    }
    
    /// Check if value is Null
    pub fn is_null(&self) -> bool {
        matches!(self, ArangoValue::Null)
    }
    
    /// Check if value is a boolean
    pub fn is_bool(&self) -> bool {
        matches!(self, ArangoValue::Bool(_))
    }
    
    /// Check if value is a number (Int, UInt, or Double)
    pub fn is_number(&self) -> bool {
        matches!(self, ArangoValue::Int(_) | ArangoValue::UInt(_) | ArangoValue::Double(_))
    }
    
    /// Check if value is a string
    pub fn is_string(&self) -> bool {
        matches!(self, ArangoValue::String(_))
    }
    
    /// Check if value is an array
    pub fn is_array(&self) -> bool {
        matches!(self, ArangoValue::Array(_) | ArangoValue::Range { .. })
    }
    
    /// Check if value is an object
    pub fn is_object(&self) -> bool {
        matches!(self, ArangoValue::Object(_))
    }
    
    /// Check if value is binary data
    pub fn is_binary(&self) -> bool {
        matches!(self, ArangoValue::Binary(_))
    }
    
    /// Check if value is a range
    pub fn is_range(&self) -> bool {
        matches!(self, ArangoValue::Range { .. })
    }
    
    /// Check if value is a datetime
    pub fn is_datetime(&self) -> bool {
        matches!(self, ArangoValue::DateTime(_))
    }
    
    /// Get the type name as a string
    pub fn type_name(&self) -> &'static str {
        match self {
            ArangoValue::None => "none",
            ArangoValue::Null => "null",
            ArangoValue::Bool(_) => "bool",
            ArangoValue::Int(_) => "int",
            ArangoValue::UInt(_) => "uint",
            ArangoValue::Double(_) => "double",
            ArangoValue::String(_) => "string",
            ArangoValue::Array(_) => "array",
            ArangoValue::Object(_) => "object",
            ArangoValue::Binary(_) => "binary",
            ArangoValue::Range { .. } => "range",
            ArangoValue::DateTime(_) => "datetime",
        }
    }
    
    /// Convert to boolean (ArangoDB truthiness rules)
    pub fn to_bool(&self) -> bool {
        match self {
            ArangoValue::None | ArangoValue::Null => false,
            ArangoValue::Bool(b) => *b,
            ArangoValue::Int(i) => *i != 0,
            ArangoValue::UInt(u) => *u != 0,
            ArangoValue::Double(d) => *d != 0.0 && !d.is_nan(),
            ArangoValue::String(s) => !s.is_empty(),
            ArangoValue::Array(a) => !a.is_empty(),
            ArangoValue::Object(o) => !o.is_empty(),
            ArangoValue::Binary(b) => !b.is_empty(),
            ArangoValue::Range { low, high } => low != high,
            ArangoValue::DateTime(dt) => *dt != 0,
        }
    }
    
    /// Convert to number (returns f64)
    pub fn to_number(&self) -> Result<f64> {
        match self {
            ArangoValue::Int(i) => Ok(*i as f64),
            ArangoValue::UInt(u) => Ok(*u as f64),
            ArangoValue::Double(d) => Ok(*d),
            ArangoValue::Bool(b) => Ok(if *b { 1.0 } else { 0.0 }),
            ArangoValue::String(s) => {
                s.parse::<f64>().map_err(|_| ArangoError::bad_parameter("Cannot convert string to number"))
            },
            ArangoValue::DateTime(dt) => Ok(*dt as f64),
            _ => Err(ArangoError::bad_parameter(format!("Cannot convert {} to number", self.type_name()))),
        }
    }
    
    /// Convert to string
    pub fn to_string(&self) -> String {
        match self {
            ArangoValue::None => "".to_string(),
            ArangoValue::Null => "null".to_string(),
            ArangoValue::Bool(b) => b.to_string(),
            ArangoValue::Int(i) => i.to_string(),
            ArangoValue::UInt(u) => u.to_string(),
            ArangoValue::Double(d) => d.to_string(),
            ArangoValue::String(s) => s.clone(),
            ArangoValue::Array(_) => "[Array]".to_string(),
            ArangoValue::Object(_) => "[Object]".to_string(),
            ArangoValue::Binary(b) => format!("[Binary {} bytes]", b.len()),
            ArangoValue::Range { low, high } => format!("[Range {}..{}]", low, high),
            ArangoValue::DateTime(dt) => format!("[DateTime {}]", dt),
        }
    }
    
    /// Get array length (for arrays and ranges)
    pub fn length(&self) -> Result<usize> {
        match self {
            ArangoValue::Array(a) => Ok(a.len()),
            ArangoValue::Range { low, high } => {
                if high >= low {
                    Ok((high - low + 1) as usize)
                } else {
                    Ok(0)
                }
            },
            ArangoValue::String(s) => Ok(s.chars().count()),
            ArangoValue::Object(o) => Ok(o.len()),
            ArangoValue::Binary(b) => Ok(b.len()),
            _ => Err(ArangoError::bad_parameter(format!("{} has no length", self.type_name()))),
        }
    }
    
    /// Get array element at index
    pub fn at(&self, index: i64) -> Result<ArangoValue> {
        match self {
            ArangoValue::Array(a) => {
                let idx = if index < 0 {
                    if (-index) as usize > a.len() {
                        return Err(ArangoError::bad_parameter("Array index out of bounds"));
                    }
                    a.len() - (-index) as usize
                } else {
                    index as usize
                };
                
                a.get(idx)
                    .cloned()
                    .ok_or_else(|| ArangoError::bad_parameter("Array index out of bounds"))
            },
            ArangoValue::Range { low, high } => {
                if index >= 0 && *low + index <= *high {
                    Ok(ArangoValue::Int(*low + index))
                } else {
                    Err(ArangoError::bad_parameter("Range index out of bounds"))
                }
            },
            _ => Err(ArangoError::bad_parameter(format!("{} is not indexable", self.type_name()))),
        }
    }
    
    /// Get object attribute by name
    pub fn get(&self, key: &str) -> Result<ArangoValue> {
        match self {
            ArangoValue::Object(o) => {
                Ok(o.get(key).cloned().unwrap_or(ArangoValue::Null))
            },
            _ => Err(ArangoError::bad_parameter(format!("{} has no attributes", self.type_name()))),
        }
    }
    
    /// Check if object has attribute
    pub fn has(&self, key: &str) -> bool {
        match self {
            ArangoValue::Object(o) => o.contains_key(key),
            _ => false,
        }
    }
}

impl Default for ArangoValue {
    fn default() -> Self {
        ArangoValue::None
    }
}

impl fmt::Display for ArangoValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.to_string())
    }
}

/// Convert from serde_json::Value to ArangoValue
impl From<JsonValue> for ArangoValue {
    fn from(value: JsonValue) -> Self {
        match value {
            JsonValue::Null => ArangoValue::Null,
            JsonValue::Bool(b) => ArangoValue::Bool(b),
            JsonValue::Number(n) => {
                if let Some(i) = n.as_i64() {
                    ArangoValue::Int(i)
                } else if let Some(u) = n.as_u64() {
                    ArangoValue::UInt(u)
                } else if let Some(f) = n.as_f64() {
                    ArangoValue::Double(f)
                } else {
                    ArangoValue::Double(0.0)
                }
            },
            JsonValue::String(s) => ArangoValue::String(s),
            JsonValue::Array(a) => {
                ArangoValue::Array(a.into_iter().map(ArangoValue::from).collect())
            },
            JsonValue::Object(o) => {
                let mut map = HashMap::new();
                for (k, v) in o {
                    map.insert(k, ArangoValue::from(v));
                }
                ArangoValue::Object(map)
            },
        }
    }
}

/// Convert from ArangoValue to serde_json::Value
impl From<ArangoValue> for JsonValue {
    fn from(value: ArangoValue) -> Self {
        match value {
            ArangoValue::None | ArangoValue::Null => JsonValue::Null,
            ArangoValue::Bool(b) => JsonValue::Bool(b),
            ArangoValue::Int(i) => JsonValue::Number(serde_json::Number::from(i)),
            ArangoValue::UInt(u) => JsonValue::Number(serde_json::Number::from(u)),
            ArangoValue::Double(d) => {
                if let Some(n) = serde_json::Number::from_f64(d) {
                    JsonValue::Number(n)
                } else {
                    JsonValue::Null
                }
            },
            ArangoValue::String(s) => JsonValue::String(s),
            ArangoValue::Array(a) => {
                JsonValue::Array(a.into_iter().map(JsonValue::from).collect())
            },
            ArangoValue::Object(o) => {
                let mut map = serde_json::Map::new();
                for (k, v) in o {
                    map.insert(k, JsonValue::from(v));
                }
                JsonValue::Object(map)
            },
            ArangoValue::Binary(_) => {
                // Binary data can't be directly represented in JSON
                // In real ArangoDB this would be base64 encoded
                JsonValue::Null
            },
            ArangoValue::Range { low, high } => {
                // Ranges are materialized as arrays in JSON
                let mut arr = Vec::new();
                for i in low..=high {
                    arr.push(JsonValue::Number(serde_json::Number::from(i)));
                }
                JsonValue::Array(arr)
            },
            ArangoValue::DateTime(dt) => {
                JsonValue::Number(serde_json::Number::from(dt))
            },
        }
    }
}

/// Serialize ArangoValue to JSON
impl Serialize for ArangoValue {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let json_value: JsonValue = self.clone().into();
        json_value.serialize(serializer)
    }
}

/// Deserialize ArangoValue from JSON
impl<'de> Deserialize<'de> for ArangoValue {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let json_value = JsonValue::deserialize(deserializer)?;
        Ok(ArangoValue::from(json_value))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_value_creation() {
        assert!(ArangoValue::none().is_none());
        assert!(ArangoValue::null().is_null());
        assert!(ArangoValue::bool(true).is_bool());
        assert!(ArangoValue::int(42).is_number());
        assert!(ArangoValue::string("hello").is_string());
        assert!(ArangoValue::array(vec![]).is_array());
        assert!(ArangoValue::object(HashMap::new()).is_object());
    }

    #[test]
    fn test_json_conversion() {
        let value = ArangoValue::object({
            let mut map = HashMap::new();
            map.insert("name".to_string(), ArangoValue::string("test"));
            map.insert("age".to_string(), ArangoValue::int(25));
            map.insert("active".to_string(), ArangoValue::bool(true));
            map
        });

        let json: JsonValue = value.clone().into();
        let back: ArangoValue = json.into();
        
        assert_eq!(value, back);
    }

    #[test]
    fn test_range_functionality() {
        let range = ArangoValue::range(1, 5);
        assert_eq!(range.length().unwrap(), 5);
        assert_eq!(range.at(0).unwrap(), ArangoValue::int(1));
        assert_eq!(range.at(4).unwrap(), ArangoValue::int(5));
    }

    #[test]
    fn test_array_indexing() {
        let arr = ArangoValue::array(vec![
            ArangoValue::int(10),
            ArangoValue::int(20),
            ArangoValue::int(30),
        ]);
        
        assert_eq!(arr.at(0).unwrap(), ArangoValue::int(10));
        assert_eq!(arr.at(-1).unwrap(), ArangoValue::int(30));
        assert!(arr.at(5).is_err());
    }

    #[test]
    fn test_truthiness() {
        assert!(!ArangoValue::none().to_bool());
        assert!(!ArangoValue::null().to_bool());
        assert!(!ArangoValue::bool(false).to_bool());
        assert!(ArangoValue::bool(true).to_bool());
        assert!(!ArangoValue::int(0).to_bool());
        assert!(ArangoValue::int(1).to_bool());
        assert!(!ArangoValue::string("").to_bool());
        assert!(ArangoValue::string("hello").to_bool());
    }
} 