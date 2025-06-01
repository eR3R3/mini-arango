use std::sync::Arc;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use crate::common::error::{ArangoError, Result};

/// Validation levels for schema validation
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[repr(u32)]
pub enum ValidationLevel {
    None = 0,
    New = 1,
    Moderate = 2,
    Strict = 3,
}

impl ValidationLevel {
    pub fn from_str(s: &str) -> Result<Self> {
        match s {
            "none" => Ok(ValidationLevel::None),
            "new" => Ok(ValidationLevel::New), 
            "moderate" => Ok(ValidationLevel::Moderate),
            "strict" => Ok(ValidationLevel::Strict),
            _ => Err(ArangoError::bad_parameter(format!("Invalid validation level: {}", s)))
        }
    }
    
    pub fn as_str(&self) -> &'static str {
        match self {
            ValidationLevel::None => "none",
            ValidationLevel::New => "new",
            ValidationLevel::Moderate => "moderate", 
            ValidationLevel::Strict => "strict",
        }
    }
}

impl Default for ValidationLevel {
    fn default() -> Self {
        ValidationLevel::None
    }
}

/// Base trait for all validators
pub trait ValidatorBase: Send + Sync {
    /// Validate a document considering the validation level and operation type
    fn validate(&self, new_doc: &JsonValue, old_doc: Option<&JsonValue>, is_insert: bool) -> Result<()>;
    
    /// Validate a single document ignoring the level (used for AQL functions)
    fn validate_one(&self, document: &JsonValue) -> Result<()>;
    
    /// Get the validator type
    fn validator_type(&self) -> &'static str;
    
    /// Get the validation message
    fn message(&self) -> &str;
    
    /// Get the validation level
    fn level(&self) -> ValidationLevel;
    
    /// Set the validation level
    fn set_level(&mut self, level: ValidationLevel);
    
    /// Serialize to JSON
    fn to_json(&self) -> JsonValue;
}

/// JSON Schema validator implementation
#[derive(Debug)]
pub struct JsonSchemaValidator {
    schema: JsonValue,
    message: String,
    level: ValidationLevel,
}

impl JsonSchemaValidator {
    pub fn new(schema: JsonValue, message: Option<String>, level: Option<ValidationLevel>) -> Result<Self> {
        if !schema.is_object() {
            return Err(ArangoError::bad_parameter("Schema must be an object"));
        }
        
        // Validate schema format (simplified)
        Self::validate_schema_format(&schema)?;
        
        Ok(JsonSchemaValidator {
            schema,
            message: message.unwrap_or_else(|| "Document validation failed".to_string()),
            level: level.unwrap_or(ValidationLevel::Strict),
        })
    }
    
    fn validate_schema_format(schema: &JsonValue) -> Result<()> {
        // Basic schema validation (simplified version of ArangoDB's validation)
        if let Some(obj) = schema.as_object() {
            // Check for valid JSON Schema keywords
            let valid_keywords = [
                "type", "properties", "required", "additionalProperties", 
                "minimum", "maximum", "minLength", "maxLength", "pattern",
                "items", "minItems", "maxItems", "enum", "const",
                "$schema", "title", "description", "default", "examples"
            ];
            
            for key in obj.keys() {
                if !valid_keywords.contains(&key.as_str()) && !key.starts_with('$') {
                    // Allow custom properties starting with 'x-'
                    if !key.starts_with("x-") {
                        return Err(ArangoError::bad_parameter(
                            format!("Unknown schema keyword: {}", key)
                        ));
                    }
                }
            }
        }
        
        Ok(())
    }
    
    fn validate_against_schema(&self, document: &JsonValue) -> Result<()> {
        // Simplified JSON Schema validation
        // In real ArangoDB this uses a proper JSON Schema library
        
        if let Some(schema_obj) = self.schema.as_object() {
            // Type validation
            if let Some(type_val) = schema_obj.get("type") {
                if let Some(type_str) = type_val.as_str() {
                    let valid = match type_str {
                        "object" => document.is_object(),
                        "array" => document.is_array(),
                        "string" => document.is_string(),
                        "number" => document.is_number(),
                        "integer" => document.is_i64() || document.is_u64(),
                        "boolean" => document.is_boolean(),
                        "null" => document.is_null(),
                        _ => true, // Allow unknown types for now
                    };
                    
                    if !valid {
                        return Err(ArangoError::bad_parameter(
                            format!("Document type doesn't match schema. Expected: {}", type_str)
                        ));
                    }
                }
            }
            
            // Properties validation (for objects)
            if document.is_object() && schema_obj.contains_key("properties") {
                if let Some(properties) = schema_obj.get("properties").and_then(|p| p.as_object()) {
                    if let Some(doc_obj) = document.as_object() {
                        for (prop_name, prop_schema) in properties {
                            if let Some(prop_value) = doc_obj.get(prop_name) {
                                // Recursively validate property
                                let prop_validator = JsonSchemaValidator::new(
                                    prop_schema.clone(),
                                    Some(format!("Property '{}' validation failed", prop_name)),
                                    Some(self.level)
                                )?;
                                prop_validator.validate_against_schema(prop_value)?;
                            }
                        }
                    }
                }
                
                // Required properties validation
                if let Some(required) = schema_obj.get("required").and_then(|r| r.as_array()) {
                    if let Some(doc_obj) = document.as_object() {
                        for req_prop in required {
                            if let Some(prop_name) = req_prop.as_str() {
                                if !doc_obj.contains_key(prop_name) {
                                    return Err(ArangoError::bad_parameter(
                                        format!("Required property '{}' is missing", prop_name)
                                    ));
                                }
                            }
                        }
                    }
                }
            }
            
            // String validation
            if document.is_string() {
                let string_val = document.as_str().unwrap();
                
                if let Some(min_length) = schema_obj.get("minLength").and_then(|v| v.as_u64()) {
                    if (string_val.len() as u64) < min_length {
                        return Err(ArangoError::bad_parameter(
                            format!("String too short. Minimum length: {}", min_length)
                        ));
                    }
                }
                
                if let Some(max_length) = schema_obj.get("maxLength").and_then(|v| v.as_u64()) {
                    if (string_val.len() as u64) > max_length {
                        return Err(ArangoError::bad_parameter(
                            format!("String too long. Maximum length: {}", max_length)
                        ));
                    }
                }
                
                if let Some(pattern) = schema_obj.get("pattern").and_then(|v| v.as_str()) {
                    // Basic pattern matching (simplified)
                    if let Ok(regex) = regex::Regex::new(pattern) {
                        if !regex.is_match(string_val) {
                            return Err(ArangoError::bad_parameter(
                                format!("String doesn't match pattern: {}", pattern)
                            ));
                        }
                    }
                }
            }
            
            // Number validation
            if document.is_number() {
                let num_val = document.as_f64().unwrap();
                
                if let Some(minimum) = schema_obj.get("minimum").and_then(|v| v.as_f64()) {
                    if num_val < minimum {
                        return Err(ArangoError::bad_parameter(
                            format!("Number too small. Minimum: {}", minimum)
                        ));
                    }
                }
                
                if let Some(maximum) = schema_obj.get("maximum").and_then(|v| v.as_f64()) {
                    if num_val > maximum {
                        return Err(ArangoError::bad_parameter(
                            format!("Number too large. Maximum: {}", maximum)
                        ));
                    }
                }
            }
            
            // Array validation
            if document.is_array() {
                if let Some(arr) = document.as_array() {
                    if let Some(min_items) = schema_obj.get("minItems").and_then(|v| v.as_u64()) {
                        if (arr.len() as u64) < min_items {
                            return Err(ArangoError::bad_parameter(
                                format!("Array too short. Minimum items: {}", min_items)
                            ));
                        }
                    }
                    
                    if let Some(max_items) = schema_obj.get("maxItems").and_then(|v| v.as_u64()) {
                        if (arr.len() as u64) > max_items {
                            return Err(ArangoError::bad_parameter(
                                format!("Array too long. Maximum items: {}", max_items)
                            ));
                        }
                    }
                    
                    // Validate array items
                    if let Some(items_schema) = schema_obj.get("items") {
                        for (index, item) in arr.iter().enumerate() {
                            let item_validator = JsonSchemaValidator::new(
                                items_schema.clone(),
                                Some(format!("Array item {} validation failed", index)),
                                Some(self.level)
                            )?;
                            item_validator.validate_against_schema(item)?;
                        }
                    }
                }
            }
            
            // Enum validation
            if let Some(enum_values) = schema_obj.get("enum").and_then(|v| v.as_array()) {
                if !enum_values.contains(document) {
                    return Err(ArangoError::bad_parameter(
                        "Document value is not in allowed enum values".to_string()
                    ));
                }
            }
        }
        
        Ok(())
    }
}

impl ValidatorBase for JsonSchemaValidator {
    fn validate(&self, new_doc: &JsonValue, old_doc: Option<&JsonValue>, is_insert: bool) -> Result<()> {
        match self.level {
            ValidationLevel::None => Ok(()),
            ValidationLevel::New => {
                if is_insert {
                    self.validate_one(new_doc)
                } else {
                    Ok(()) // Only validate inserts
                }
            }
            ValidationLevel::Strict => {
                self.validate_one(new_doc)
            }
            ValidationLevel::Moderate => {
                // Moderate: only validate if old document was valid or this is an insert
                if is_insert {
                    self.validate_one(new_doc)
                } else if let Some(old) = old_doc {
                    // Check if old document was valid
                    let old_valid = self.validate_one(old).is_ok();
                    if old_valid {
                        // Old was valid, so new must be valid too
                        self.validate_one(new_doc)
                    } else {
                        // Old was invalid, so we don't enforce validation on new
                        Ok(())
                    }
                } else {
                    self.validate_one(new_doc)
                }
            }
        }
    }
    
    fn validate_one(&self, document: &JsonValue) -> Result<()> {
        self.validate_against_schema(document).map_err(|_| {
            ArangoError::bad_parameter(self.message.clone())
        })
    }
    
    fn validator_type(&self) -> &'static str {
        "json"
    }
    
    fn message(&self) -> &str {
        &self.message
    }
    
    fn level(&self) -> ValidationLevel {
        self.level
    }
    
    fn set_level(&mut self, level: ValidationLevel) {
        self.level = level;
    }
    
    fn to_json(&self) -> JsonValue {
        serde_json::json!({
            "type": self.validator_type(),
            "rule": self.schema,
            "message": self.message,
            "level": self.level.as_str()
        })
    }
}

/// Boolean validator (always returns true or false)
#[derive(Debug)]
pub struct BooleanValidator {
    result: bool,
    message: String,
    level: ValidationLevel,
}

impl BooleanValidator {
    pub fn new(result: bool, message: Option<String>, level: Option<ValidationLevel>) -> Self {
        BooleanValidator {
            result,
            message: message.unwrap_or_else(|| "Boolean validation failed".to_string()),
            level: level.unwrap_or(ValidationLevel::Strict),
        }
    }
}

impl ValidatorBase for BooleanValidator {
    fn validate(&self, new_doc: &JsonValue, old_doc: Option<&JsonValue>, is_insert: bool) -> Result<()> {
        match self.level {
            ValidationLevel::None => Ok(()),
            ValidationLevel::New => {
                if is_insert {
                    self.validate_one(new_doc)
                } else {
                    Ok(())
                }
            }
            ValidationLevel::Strict => self.validate_one(new_doc),
            ValidationLevel::Moderate => {
                if is_insert {
                    self.validate_one(new_doc)
                } else if let Some(old) = old_doc {
                    let old_valid = self.validate_one(old).is_ok();
                    if old_valid {
                        self.validate_one(new_doc)
                    } else {
                        Ok(())
                    }
                } else {
                    self.validate_one(new_doc)
                }
            }
        }
    }
    
    fn validate_one(&self, _document: &JsonValue) -> Result<()> {
        if self.result {
            Ok(())
        } else {
            Err(ArangoError::bad_parameter(self.message.clone()))
        }
    }
    
    fn validator_type(&self) -> &'static str {
        "bool"
    }
    
    fn message(&self) -> &str {
        &self.message
    }
    
    fn level(&self) -> ValidationLevel {
        self.level
    }
    
    fn set_level(&mut self, level: ValidationLevel) {
        self.level = level;
    }
    
    fn to_json(&self) -> JsonValue {
        serde_json::json!({
            "type": self.validator_type(),
            "rule": self.result,
            "message": self.message,
            "level": self.level.as_str()
        })
    }
}

/// Validator factory for creating validators from JSON
pub struct ValidatorFactory;

impl ValidatorFactory {
    pub fn create_validator(definition: &JsonValue) -> Result<Arc<dyn ValidatorBase>> {
        let validator_type = definition.get("type")
            .and_then(|t| t.as_str())
            .unwrap_or("json"); // default to json
            
        let message = definition.get("message")
            .and_then(|m| m.as_str())
            .map(|s| s.to_string());
            
        let level = definition.get("level")
            .and_then(|l| l.as_str())
            .map(|s| ValidationLevel::from_str(s))
            .transpose()?
            .unwrap_or(ValidationLevel::Strict);
            
        match validator_type {
            "json" => {
                let rule = definition.get("rule")
                    .ok_or_else(|| ArangoError::bad_parameter("Missing 'rule' for json validator"))?;
                    
                let validator = JsonSchemaValidator::new(rule.clone(), message, Some(level))?;
                Ok(Arc::new(validator))
            }
            "bool" => {
                let rule = definition.get("rule")
                    .and_then(|r| r.as_bool())
                    .ok_or_else(|| ArangoError::bad_parameter("Missing or invalid 'rule' for bool validator"))?;
                    
                let validator = BooleanValidator::new(rule, message, Some(level));
                Ok(Arc::new(validator))
            }
            _ => Err(ArangoError::bad_parameter(format!("Unknown validator type: {}", validator_type)))
        }
    }
    
    /// Create a validator from a schema object (convenience method)
    pub fn create_json_schema_validator(schema: &JsonValue, level: ValidationLevel) -> Result<Arc<dyn ValidatorBase>> {
        let validator = JsonSchemaValidator::new(schema.clone(), None, Some(level))?;
        Ok(Arc::new(validator))
    }
    
    /// Check if two validator definitions are the same
    pub fn is_same_validator(def1: &JsonValue, def2: &JsonValue) -> bool {
        // Compare type, rule, message, and level
        let type1 = def1.get("type").and_then(|t| t.as_str()).unwrap_or("json");
        let type2 = def2.get("type").and_then(|t| t.as_str()).unwrap_or("json");
        
        if type1 != type2 {
            return false;
        }
        
        let rule1 = def1.get("rule");
        let rule2 = def2.get("rule");
        if rule1 != rule2 {
            return false;
        }
        
        let message1 = def1.get("message");
        let message2 = def2.get("message");
        if message1 != message2 {
            return false;
        }
        
        let level1 = def1.get("level");
        let level2 = def2.get("level");
        if level1 != level2 {
            return false;
        }
        
        true
    }
} 