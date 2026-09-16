use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

pub const PROTOCOL_VERSION: i32 = 1;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "Type")]
pub enum Field {
    #[serde(rename = "Method")]
    Method(MethodField),
    #[serde(rename = "Event")]
    Event(EventField),
}

impl Field {
    pub fn id(&self) -> &str {
        match self {
            Self::Method(field) => &field.id,
            Self::Event(field) => &field.id,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct MethodField {
    pub id: String,
    #[serde(default)]
    pub friendly_name: Option<String>,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub query_schema: Option<Value>,
    #[serde(default)]
    pub receipt_schema: Option<Value>,
}

impl MethodField {
    pub fn new(id: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            friendly_name: None,
            description: None,
            query_schema: None,
            receipt_schema: None,
        }
    }

    pub fn friendly_name(mut self, friendly_name: impl Into<String>) -> Self {
        self.friendly_name = Some(friendly_name.into());
        self
    }

    pub fn description(mut self, description: impl Into<String>) -> Self {
        self.description = Some(description.into());
        self
    }

    pub fn query_schema(mut self, schema: Value) -> Self {
        self.query_schema = Some(schema);
        self
    }

    pub fn receipt_schema(mut self, schema: Value) -> Self {
        self.receipt_schema = Some(schema);
        self
    }

    pub fn is_action(&self) -> bool {
        self.query_schema.is_none() && self.receipt_schema.is_none()
    }

    pub fn is_actable(&self) -> bool {
        self.query_schema.is_none()
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct EventField {
    pub id: String,
    #[serde(default)]
    pub friendly_name: Option<String>,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub call_arg_schema: Option<Value>,
}

impl EventField {
    pub fn new(id: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            friendly_name: None,
            description: None,
            call_arg_schema: None,
        }
    }

    pub fn friendly_name(mut self, friendly_name: impl Into<String>) -> Self {
        self.friendly_name = Some(friendly_name.into());
        self
    }

    pub fn description(mut self, description: impl Into<String>) -> Self {
        self.description = Some(description.into());
        self
    }

    pub fn call_arg_schema(mut self, schema: Value) -> Self {
        self.call_arg_schema = Some(schema);
        self
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "Type")]
pub enum Glycosyl {
    #[serde(rename = "Beacon")]
    Beacon(Beacon),
    #[serde(rename = "Query")]
    Query(Query),
    #[serde(rename = "Reply")]
    Reply(Reply),
    #[serde(rename = "Event")]
    Event(Event),
    #[serde(rename = "Heartbeat")]
    Heartbeat(Heartbeat),
}

impl Glycosyl {
    pub fn to_bytes(&self) -> serde_json::Result<Vec<u8>> {
        serde_json::to_vec(self)
    }

    pub fn from_bytes(bytes: &[u8]) -> serde_json::Result<Self> {
        serde_json::from_slice(bytes)
    }

    pub fn receiver(&self) -> Option<&str> {
        match self {
            Self::Query(query) => Some(&query.gid),
            Self::Reply(reply) => reply.target_gid.as_deref(),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct Beacon {
    pub id: String,
    #[serde(default)]
    pub vendor: Option<String>,
    #[serde(default)]
    pub protocol_version: Option<i32>,
    pub fields: Vec<Field>,
    pub timestamp: DateTime<Utc>,
}

impl Beacon {
    pub fn new(id: impl Into<String>, fields: Vec<Field>) -> Self {
        Self {
            id: id.into(),
            vendor: None,
            protocol_version: Some(PROTOCOL_VERSION),
            fields,
            timestamp: Utc::now(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct Query {
    pub gid: String,
    #[serde(default)]
    pub source_gid: Option<String>,
    pub fid: String,
    #[serde(default)]
    pub payload: Option<Value>,
    #[serde(default = "Uuid::new_v4")]
    pub qid: Uuid,
}

impl Query {
    pub fn new(
        gid: impl Into<String>,
        source_gid: impl Into<String>,
        fid: impl Into<String>,
        payload: Option<Value>,
    ) -> Self {
        Self {
            gid: gid.into(),
            source_gid: Some(source_gid.into()),
            fid: fid.into(),
            payload,
            qid: Uuid::new_v4(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct Reply {
    #[serde(default)]
    pub payload: Option<Value>,
    pub qid: Uuid,
    #[serde(default)]
    pub target_gid: Option<String>,
    #[serde(default)]
    pub error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct Event {
    pub gid: String,
    pub fid: String,
    #[serde(default)]
    pub arg: Option<Value>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct Heartbeat {
    pub id: String,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn query_uses_dotnet_property_names_and_discriminator() {
        let qid = Uuid::parse_str("00112233-4455-6677-8899-aabbccddeeff").unwrap();
        let message = Glycosyl::Query(Query {
            gid: "beta".into(),
            source_gid: Some("alpha".into()),
            fid: "add".into(),
            payload: Some(json!({"A": 1, "B": 2})),
            qid,
        });

        let value = serde_json::to_value(message).unwrap();
        assert_eq!(value["Type"], "Query");
        assert_eq!(value["Gid"], "beta");
        assert_eq!(value["SourceGid"], "alpha");
        assert_eq!(value["Fid"], "add");
        assert_eq!(value["Payload"]["A"], 1);
        assert_eq!(value["Qid"], qid.to_string());
    }

    #[test]
    fn optional_values_are_serialized_as_null_like_system_text_json() {
        let message = Glycosyl::Reply(Reply {
            payload: None,
            qid: Uuid::nil(),
            target_gid: None,
            error: None,
        });

        let value = serde_json::to_value(message).unwrap();
        assert!(value.get("Payload").unwrap().is_null());
        assert!(value.get("TargetGid").unwrap().is_null());
        assert!(value.get("Error").unwrap().is_null());
    }

    #[test]
    fn field_discriminator_is_flattened() {
        let value = serde_json::to_value(Field::Method(MethodField::new("ping"))).unwrap();
        assert_eq!(value["Type"], "Method");
        assert_eq!(value["Id"], "ping");
        assert!(value["QuerySchema"].is_null());
        assert!(value["ReceiptSchema"].is_null());
    }

    #[test]
    fn round_trips_every_message_kind() {
        let values = vec![
            Glycosyl::Beacon(Beacon::new("alpha", vec![])),
            Glycosyl::Query(Query::new("beta", "alpha", "ping", None)),
            Glycosyl::Reply(Reply {
                payload: Some(json!({"Message": "pong"})),
                qid: Uuid::new_v4(),
                target_gid: Some("alpha".into()),
                error: None,
            }),
            Glycosyl::Event(Event {
                gid: "alpha".into(),
                fid: "ready".into(),
                arg: None,
            }),
            Glycosyl::Heartbeat(Heartbeat { id: "alpha".into() }),
        ];

        for value in values {
            let bytes = value.to_bytes().unwrap();
            assert_eq!(Glycosyl::from_bytes(&bytes).unwrap(), value);
        }
    }
}
