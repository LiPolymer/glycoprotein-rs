//! A Rust implementation of the Glycoprotein v1 local IPC protocol.

//! Rust implementation of the Glycoprotein protocol-v1 local IPC mesh.
//!
//! [`GlycoComplex`] is the high-level API. It discovers peers over Unix-domain
//! sockets, publishes callable fields and events, and interoperates with the
//! existing .NET implementation without changing the wire protocol.

mod error;
mod node;
mod protocol;
mod schema;
mod transport;

pub use error::{GlycoError, HandlerError, RemoteError, Result};
pub use node::{CallOptions, GlycoComplex, GlycoComplexBuilder, PresenterEvent, RequestContext};
pub use protocol::{
    Beacon, Event, EventField, Field, Glycosyl, Heartbeat, MethodField, PROTOCOL_VERSION, Query,
    Reply,
};
pub use schema::{generate_schema, validate_json};
pub use transport::{Connexon, ConnexonEvent, UnixDomainMeshConnexon};
