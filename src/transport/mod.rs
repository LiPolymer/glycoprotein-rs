mod unix_mesh;

use async_trait::async_trait;
use tokio::sync::broadcast;

use crate::{Glycosyl, Result};

pub use unix_mesh::UnixDomainMeshConnexon;

#[derive(Debug, Clone, PartialEq)]
pub enum ConnexonEvent {
    Message(Glycosyl),
    PeerConnected(String),
    PeerDisconnected(String),
    Fault {
        peer: Option<String>,
        message: String,
    },
}

#[async_trait]
pub trait Connexon: Send + Sync {
    fn node_id(&self) -> &str;

    fn subscribe(&self) -> broadcast::Receiver<ConnexonEvent>;

    async fn start(&self) -> Result<()>;

    async fn stop(&self) -> Result<()>;

    async fn send(&self, message: &Glycosyl) -> Result<()>;

    async fn send_bytes(&self, data: &[u8], receiver: Option<&str>) -> Result<()>;
}
