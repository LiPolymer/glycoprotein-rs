use schemars::JsonSchema;
use serde_json::Value;

use crate::{GlycoError, Result};

pub fn generate_schema<T: JsonSchema>() -> Result<Value> {
    serde_json::to_value(schemars::schema_for!(T))
        .map_err(|error| GlycoError::SchemaGeneration(error.to_string()))
}

pub fn validate_json(schema: &Value, instance: &Value) -> Result<()> {
    let validator = jsonschema::validator_for(schema)
        .map_err(|error| GlycoError::Validation(error.to_string()))?;
    let errors: Vec<String> = validator
        .iter_errors(instance)
        .map(|error| error.to_string())
        .collect();

    if errors.is_empty() {
        Ok(())
    } else {
        Err(GlycoError::Validation(errors.join("\n")))
    }
}
