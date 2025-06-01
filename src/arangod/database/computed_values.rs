use std::collections::HashMap;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use crate::common::error::{ArangoError, Result};

/// When to compute values
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ComputeValuesOn {
    Never = 0,
    Insert = 1,
    Update = 2,
    Replace = 4,
}

impl ComputeValuesOn {
    pub fn from_str(s: &str) -> Result<Self> {
        match s {
            "insert" => Ok(ComputeValuesOn::Insert),
            "update" => Ok(ComputeValuesOn::Update),
            "replace" => Ok(ComputeValuesOn::Replace),
            _ => Err(ArangoError::bad_parameter(format!("Invalid computeOn value: {}", s)))
        }
    }
    
    pub fn as_str(&self) -> &'static str {
        match self {
            ComputeValuesOn::Never => "never",
            ComputeValuesOn::Insert => "insert",
            ComputeValuesOn::Update => "update",
            ComputeValuesOn::Replace => "replace",
        }
    }
    
    pub fn as_u8(&self) -> u8 {
        *self as u8
    }
    
    pub fn contains(&self, other: ComputeValuesOn) -> bool {
        (self.as_u8() & other.as_u8()) != 0
    }
    
    pub fn combine(&self, other: ComputeValuesOn) -> ComputeValuesOn {
        match self.as_u8() | other.as_u8() {
            1 => ComputeValuesOn::Insert,
            2 => ComputeValuesOn::Update,
            3 => ComputeValuesOn::Insert, // Insert | Update
            4 => ComputeValuesOn::Replace,
            5 => ComputeValuesOn::Insert, // Insert | Replace
            6 => ComputeValuesOn::Update, // Update | Replace
            7 => ComputeValuesOn::Insert, // Insert | Update | Replace
            _ => ComputeValuesOn::Never,
        }
    }
}

/// Individual computed value definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ComputedValue {
    pub name: String,
    pub expression: String,
    pub compute_on: Vec<ComputeValuesOn>,
    pub overwrite: bool,
    pub fail_on_warning: bool,
    pub keep_null: bool,
}

impl ComputedValue {
    pub fn new(
        name: String,
        expression: String,
        compute_on: Vec<ComputeValuesOn>,
        overwrite: bool,
        fail_on_warning: bool,
        keep_null: bool,
    ) -> Self {
        ComputedValue {
            name,
            expression,
            compute_on,
            overwrite,
            fail_on_warning,
            keep_null,
        }
    }
    
    pub fn should_compute_on(&self, operation: ComputeValuesOn) -> bool {
        self.compute_on.contains(&operation)
    }
    
    pub fn compute_attribute(&self, input: &JsonValue) -> Result<Option<JsonValue>> {
        // Simplified expression evaluation
        // In real ArangoDB this would use the AQL expression engine
        
        // For now, we'll implement some basic expressions
        let trimmed_expr = self.expression.trim();
        
        // Handle simple literals
        if trimmed_expr.starts_with('"') && trimmed_expr.ends_with('"') {
            // String literal
            let value = trimmed_expr[1..trimmed_expr.len()-1].to_string();
            return Ok(Some(JsonValue::String(value)));
        }
        
        // Handle numeric literals
        if let Ok(num) = trimmed_expr.parse::<f64>() {
            return Ok(Some(JsonValue::Number(serde_json::Number::from_f64(num).unwrap())));
        }
        
        // Handle boolean literals
        if trimmed_expr == "true" {
            return Ok(Some(JsonValue::Bool(true)));
        }
        if trimmed_expr == "false" {
            return Ok(Some(JsonValue::Bool(false)));
        }
        if trimmed_expr == "null" {
            return Ok(Some(JsonValue::Null));
        }
        
        // Handle simple field references like @doc.field
        if trimmed_expr.starts_with("@doc.") {
            let field_name = &trimmed_expr[5..];
            if let Some(obj) = input.as_object() {
                if let Some(value) = obj.get(field_name) {
                    return Ok(Some(value.clone()));
                }
            }
            return Ok(Some(JsonValue::Null));
        }
        
        // Handle simple functions
        if trimmed_expr == "NOW()" {
            let timestamp = chrono::Utc::now().timestamp();
            return Ok(Some(JsonValue::Number(serde_json::Number::from(timestamp))));
        }
        
        if trimmed_expr == "DATE_NOW()" {
            let timestamp = chrono::Utc::now().timestamp_millis();
            return Ok(Some(JsonValue::Number(serde_json::Number::from(timestamp))));
        }
        
        if trimmed_expr == "UUID()" {
            let uuid = uuid::Uuid::new_v4().to_string();
            return Ok(Some(JsonValue::String(uuid)));
        }
        
        // Handle concatenation like CONCAT(@doc.first, " ", @doc.last)
        if trimmed_expr.starts_with("CONCAT(") && trimmed_expr.ends_with(")") {
            let inner = &trimmed_expr[7..trimmed_expr.len()-1];
            let parts: Vec<&str> = inner.split(',').map(|s| s.trim()).collect();
            let mut result = String::new();
            
            for part in parts {
                if part.starts_with('"') && part.ends_with('"') {
                    // String literal
                    result.push_str(&part[1..part.len()-1]);
                } else if part.starts_with("@doc.") {
                    // Field reference
                    let field_name = &part[5..];
                    if let Some(obj) = input.as_object() {
                        if let Some(value) = obj.get(field_name) {
                            if let Some(s) = value.as_str() {
                                result.push_str(s);
                            } else {
                                result.push_str(&value.to_string());
                            }
                        }
                    }
                }
            }
            
            return Ok(Some(JsonValue::String(result)));
        }
        
        // For more complex expressions, we'd need a proper AQL parser
        // For now, return an error for unsupported expressions
        Err(ArangoError::bad_parameter(format!(
            "Unsupported computed value expression: {}", self.expression
        )))
    }
    
    pub fn to_json(&self) -> JsonValue {
        serde_json::json!({
            "name": self.name,
            "expression": self.expression,
            "computeOn": self.compute_on.iter().map(|op| op.as_str()).collect::<Vec<_>>(),
            "overwrite": self.overwrite,
            "failOnWarning": self.fail_on_warning,
            "keepNull": self.keep_null
        })
    }
}

/// Collection of computed values for a collection
#[derive(Debug, Clone)]
pub struct ComputedValues {
    values: Vec<ComputedValue>,
    attributes_for_insert: HashMap<String, usize>,
    attributes_for_update: HashMap<String, usize>,
    attributes_for_replace: HashMap<String, usize>,
}

impl ComputedValues {
    pub fn new(definitions: Vec<ComputedValue>) -> Result<Self> {
        let mut attributes_for_insert = HashMap::new();
        let mut attributes_for_update = HashMap::new();
        let mut attributes_for_replace = HashMap::new();
        
        // Validate definitions
        let mut names = std::collections::HashSet::new();
        for (index, def) in definitions.iter().enumerate() {
            // Check for duplicate names
            if !names.insert(def.name.clone()) {
                return Err(ArangoError::bad_parameter(
                    format!("Duplicate computed value name: {}", def.name)
                ));
            }
            
            // Check for system attributes
            if def.name.starts_with('_') {
                return Err(ArangoError::bad_parameter(
                    format!("Cannot compute system attribute: {}", def.name)
                ));
            }
            
            // Build operation maps
            for operation in &def.compute_on {
                match operation {
                    ComputeValuesOn::Insert => {
                        attributes_for_insert.insert(def.name.clone(), index);
                    }
                    ComputeValuesOn::Update => {
                        attributes_for_update.insert(def.name.clone(), index);
                    }
                    ComputeValuesOn::Replace => {
                        attributes_for_replace.insert(def.name.clone(), index);
                    }
                    ComputeValuesOn::Never => {}
                }
            }
        }
        
        Ok(ComputedValues {
            values: definitions,
            attributes_for_insert,
            attributes_for_update,
            attributes_for_replace,
        })
    }
    
    pub fn from_json(value: &JsonValue) -> Result<Self> {
        if value.is_null() || value.is_array() && value.as_array().unwrap().is_empty() {
            return Ok(ComputedValues {
                values: Vec::new(),
                attributes_for_insert: HashMap::new(),
                attributes_for_update: HashMap::new(),
                attributes_for_replace: HashMap::new(),
            });
        }
        
        let definitions_array = value.as_array()
            .ok_or_else(|| ArangoError::bad_parameter("computedValues must be an array"))?;
            
        let mut definitions = Vec::new();
        
        for item in definitions_array {
            let obj = item.as_object()
                .ok_or_else(|| ArangoError::bad_parameter("computedValues item must be an object"))?;
                
            let name = obj.get("name")
                .and_then(|n| n.as_str())
                .ok_or_else(|| ArangoError::bad_parameter("computedValues item must have a 'name'"))?
                .to_string();
                
            let expression = obj.get("expression")
                .and_then(|e| e.as_str())
                .ok_or_else(|| ArangoError::bad_parameter("computedValues item must have an 'expression'"))?
                .to_string();
                
            let compute_on = if let Some(compute_on_val) = obj.get("computeOn") {
                if let Some(array) = compute_on_val.as_array() {
                    let mut ops = Vec::new();
                    for op_val in array {
                        if let Some(op_str) = op_val.as_str() {
                            ops.push(ComputeValuesOn::from_str(op_str)?);
                        } else {
                            return Err(ArangoError::bad_parameter("computeOn values must be strings"));
                        }
                    }
                    ops
                } else {
                    return Err(ArangoError::bad_parameter("computeOn must be an array"));
                }
            } else {
                // Default: compute on insert, update, and replace
                vec![ComputeValuesOn::Insert, ComputeValuesOn::Update, ComputeValuesOn::Replace]
            };
            
            let overwrite = obj.get("overwrite")
                .and_then(|o| o.as_bool())
                .unwrap_or(false);
                
            let fail_on_warning = obj.get("failOnWarning")
                .and_then(|f| f.as_bool())
                .unwrap_or(false);
                
            let keep_null = obj.get("keepNull")
                .and_then(|k| k.as_bool())
                .unwrap_or(true);
                
            definitions.push(ComputedValue::new(
                name,
                expression,
                compute_on,
                overwrite,
                fail_on_warning,
                keep_null,
            ));
        }
        
        Self::new(definitions)
    }
    
    pub fn must_compute_values_on_insert(&self) -> bool {
        !self.attributes_for_insert.is_empty()
    }
    
    pub fn must_compute_values_on_update(&self) -> bool {
        !self.attributes_for_update.is_empty()
    }
    
    pub fn must_compute_values_on_replace(&self) -> bool {
        !self.attributes_for_replace.is_empty()
    }
    
    pub fn merge_computed_attributes(
        &self,
        input: &JsonValue,
        keys_written: &std::collections::HashSet<String>,
        operation: ComputeValuesOn,
    ) -> Result<JsonValue> {
        let attributes = match operation {
            ComputeValuesOn::Insert => &self.attributes_for_insert,
            ComputeValuesOn::Update => &self.attributes_for_update,
            ComputeValuesOn::Replace => &self.attributes_for_replace,
            ComputeValuesOn::Never => return Ok(input.clone()),
        };
        
        if attributes.is_empty() {
            return Ok(input.clone());
        }
        
        let mut result = if let Some(obj) = input.as_object() {
            obj.clone()
        } else {
            return Err(ArangoError::bad_parameter("Input must be an object for computed values"));
        };
        
        // Compute and add computed attributes
        for (attr_name, &value_index) in attributes {
            let computed_value = &self.values[value_index];
            
            // Skip if not overwriting and key was already written
            if !computed_value.overwrite && keys_written.contains(attr_name) {
                continue;
            }
            
            // Compute the value
            match computed_value.compute_attribute(input)? {
                Some(computed) => {
                    // Only add if keepNull is true or value is not null
                    if computed_value.keep_null || !computed.is_null() {
                        result.insert(attr_name.clone(), computed);
                    }
                }
                None => {
                    // Expression returned no value, skip
                }
            }
        }
        
        Ok(JsonValue::Object(result))
    }
    
    pub fn to_json(&self) -> JsonValue {
        if self.values.is_empty() {
            JsonValue::Array(vec![])
        } else {
            JsonValue::Array(
                self.values.iter().map(|v| v.to_json()).collect()
            )
        }
    }
    
    pub fn validate_expression(&self, expression: &str) -> Result<()> {
        // Basic expression validation
        // In real ArangoDB this would use the AQL parser
        
        if expression.trim().is_empty() {
            return Err(ArangoError::bad_parameter("Expression cannot be empty"));
        }
        
        // Check for dangerous expressions (basic security)
        let dangerous_patterns = ["eval", "exec", "import", "require", "process"];
        let expr_lower = expression.to_lowercase();
        for pattern in &dangerous_patterns {
            if expr_lower.contains(pattern) {
                return Err(ArangoError::bad_parameter(
                    format!("Expression contains potentially dangerous pattern: {}", pattern)
                ));
            }
        }
        
        // Check for valid AQL syntax (simplified)
        if expression.contains("@doc") {
            // Valid document reference
        } else if expression.starts_with('"') && expression.ends_with('"') {
            // String literal
        } else if expression.parse::<f64>().is_ok() {
            // Numeric literal
        } else if ["true", "false", "null"].contains(&expression.trim()) {
            // Boolean/null literal
        } else if expression.starts_with("CONCAT(") || 
                  expression.starts_with("NOW(") ||
                  expression.starts_with("DATE_NOW(") ||
                  expression.starts_with("UUID(") {
            // Function call
        } else {
            return Err(ArangoError::bad_parameter(
                format!("Unsupported expression syntax: {}", expression)
            ));
        }
        
        Ok(())
    }
}

impl Default for ComputedValues {
    fn default() -> Self {
        ComputedValues {
            values: Vec::new(),
            attributes_for_insert: HashMap::new(),
            attributes_for_update: HashMap::new(),
            attributes_for_replace: HashMap::new(),
        }
    }
}

/// Build computed values instance with validation
pub fn build_computed_values_instance(
    shard_keys: &[String],
    computed_values: &JsonValue,
) -> Result<Option<ComputedValues>> {
    if computed_values.is_null() {
        return Ok(None);
    }
    
    if let Some(array) = computed_values.as_array() {
        if array.is_empty() {
            return Ok(None);
        }
    }
    
    let computed_values_obj = ComputedValues::from_json(computed_values)?;
    
    // Validate that computed values don't conflict with shard keys
    for cv in &computed_values_obj.values {
        if shard_keys.contains(&cv.name) {
            return Err(ArangoError::bad_parameter(
                format!("Cannot compute values for shard key attribute: {}", cv.name)
            ));
        }
        
        // Validate the expression
        computed_values_obj.validate_expression(&cv.expression)?;
    }
    
    Ok(Some(computed_values_obj))
} 