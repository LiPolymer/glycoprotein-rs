use std::cmp::Ordering;
use std::collections::HashMap;
use std::ffi::OsStr;
use std::io::{self, Read, Write};
use std::net::Shutdown;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering as AtomicOrdering};
use std::sync::{Arc, Mutex, RwLock, Weak, mpsc};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use async_trait::async_trait;
use socket2::{Domain, SockAddr, Socket, Type};
use tokio::sync::{broadcast, oneshot};
use tokio_util::sync::CancellationToken;

use super::{Connexon, ConnexonEvent};
use crate::{GlycoError, Glycosyl, Result};

const DEFAULT_DISCOVERY_INTERVAL: Duration = Duration::from_secs(2);
const DEFAULT_MAX_FRAME_LENGTH: usize = 100_000_000;
const MAX_HANDSHAKE_LENGTH: usize = 4_096;
const ACCEPT_POLL_INTERVAL: Duration = Duration::from_millis(20);
const INTERRUPT_POLL_INTERVAL: Duration = Duration::from_millis(25);
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(5);
const WRITE_TIMEOUT: Duration = Duration::from_secs(5);

pub struct UnixDomainMeshConnexon {
    inner: Arc<Inner>,
}

struct Inner {
    node_id: String,
    socket_dir: PathBuf,
    socket_path: PathBuf,
    discovery_interval: Duration,
    max_frame_length: usize,
    events: broadcast::Sender<ConnexonEvent>,
    peers: RwLock<HashMap<String, Arc<Peer>>>,
    runtime: Mutex<RuntimeState>,
    threads: Mutex<Vec<JoinHandle<()>>>,
    next_generation: AtomicU64,
}

#[derive(Default)]
struct RuntimeState {
    cancellation: Option<CancellationToken>,
}

struct Peer {
    id: String,
    generation: u64,
    writer: Mutex<Option<mpsc::Sender<WriteRequest>>>,
    control_socket: Socket,
    closed: AtomicBool,
}

struct WriteRequest {
    data: Arc<[u8]>,
    acknowledgement: oneshot::Sender<io::Result<()>>,
}

impl UnixDomainMeshConnexon {
    pub fn new(node_id: impl Into<String>) -> Result<Self> {
        Self::with_socket_directory(node_id, Self::default_socket_directory())
    }

    pub fn with_socket_directory(
        node_id: impl Into<String>,
        socket_directory: impl Into<PathBuf>,
    ) -> Result<Self> {
        let node_id = node_id.into();
        validate_node_id(&node_id)?;
        let socket_dir = socket_directory.into();
        let socket_path = socket_dir.join(format!("{node_id}.sock"));
        // Build the native address here so an overlong or otherwise unsupported
        // path fails at construction rather than in a background worker.
        SockAddr::unix(&socket_path)?;
        let (events, _) = broadcast::channel(1_024);

        Ok(Self {
            inner: Arc::new(Inner {
                node_id,
                socket_dir,
                socket_path,
                discovery_interval: DEFAULT_DISCOVERY_INTERVAL,
                max_frame_length: DEFAULT_MAX_FRAME_LENGTH,
                events,
                peers: RwLock::new(HashMap::new()),
                runtime: Mutex::new(RuntimeState::default()),
                threads: Mutex::new(Vec::new()),
                next_generation: AtomicU64::new(1),
            }),
        })
    }

    pub fn default_socket_directory() -> PathBuf {
        std::env::temp_dir().join("glycoprotein")
    }

    pub fn socket_directory(&self) -> &Path {
        &self.inner.socket_dir
    }

    pub fn socket_path(&self) -> &Path {
        &self.inner.socket_path
    }

    pub fn connected_peers(&self) -> Vec<String> {
        let mut peers: Vec<String> = self
            .inner
            .peers
            .read()
            .expect("peer map poisoned")
            .keys()
            .cloned()
            .collect();
        peers.sort();
        peers
    }

    async fn send_framed(
        &self,
        payload: &[u8],
        receiver: Option<&str>,
        loopback: Glycosyl,
    ) -> Result<()> {
        self.inner.ensure_started()?;
        let frame: Arc<[u8]> = frame_message(payload, self.inner.max_frame_length)?.into();

        match receiver {
            Some(receiver) if receiver == self.inner.node_id => {
                let _ = self.inner.events.send(ConnexonEvent::Message(loopback));
                Ok(())
            }
            Some(receiver) => {
                let peer = self
                    .inner
                    .peers
                    .read()
                    .expect("peer map poisoned")
                    .get(receiver)
                    .cloned()
                    .ok_or_else(|| GlycoError::PeerNotConnected(receiver.to_owned()))?;
                self.inner.write_to_peer(peer, frame).await
            }
            None => {
                let _ = self.inner.events.send(ConnexonEvent::Message(loopback));
                let peers: Vec<Arc<Peer>> = self
                    .inner
                    .peers
                    .read()
                    .expect("peer map poisoned")
                    .values()
                    .cloned()
                    .collect();

                let mut pending = Vec::with_capacity(peers.len());
                for peer in peers {
                    pending.push((peer.clone(), peer.enqueue(frame.clone())));
                }

                for (peer, result) in pending {
                    let result = match result {
                        Ok(receiver) => receiver
                            .await
                            .map_err(|_| broken_pipe("peer writer stopped"))?,
                        Err(error) => Err(error),
                    };
                    if let Err(error) = result {
                        self.inner.report_fault(
                            Some(peer.id.clone()),
                            format!("broadcast write failed: {error}"),
                        );
                        self.inner.disconnect_peer(&peer.id, peer.generation);
                    }
                }
                Ok(())
            }
        }
    }
}

#[async_trait]
impl Connexon for UnixDomainMeshConnexon {
    fn node_id(&self) -> &str {
        &self.inner.node_id
    }

    fn subscribe(&self) -> broadcast::Receiver<ConnexonEvent> {
        self.inner.events.subscribe()
    }

    async fn start(&self) -> Result<()> {
        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || inner.start_blocking())
            .await
            .map_err(|error| GlycoError::Task(error.to_string()))?
    }

    async fn stop(&self) -> Result<()> {
        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || inner.stop_blocking())
            .await
            .map_err(|error| GlycoError::Task(error.to_string()))?
    }

    async fn send(&self, message: &Glycosyl) -> Result<()> {
        let bytes = message.to_bytes()?;
        self.send_framed(&bytes, message.receiver(), message.clone())
            .await
    }

    async fn send_bytes(&self, data: &[u8], receiver: Option<&str>) -> Result<()> {
        let message = Glycosyl::from_bytes(data)?;
        self.send_framed(data, receiver, message).await
    }
}

impl Drop for UnixDomainMeshConnexon {
    fn drop(&mut self) {
        self.inner.cancel_without_joining();
    }
}

impl Inner {
    fn ensure_started(&self) -> Result<()> {
        if self
            .runtime
            .lock()
            .expect("runtime state poisoned")
            .cancellation
            .is_some()
        {
            Ok(())
        } else {
            Err(GlycoError::NotStarted)
        }
    }

    fn start_blocking(self: Arc<Self>) -> Result<()> {
        let mut runtime = self.runtime.lock().expect("runtime state poisoned");
        if runtime.cancellation.is_some() {
            return Ok(());
        }

        std::fs::create_dir_all(&self.socket_dir)?;
        self.remove_stale_socket()?;

        let listener = Socket::new(Domain::UNIX, Type::STREAM, None)?;
        listener.bind(&SockAddr::unix(&self.socket_path)?)?;
        listener.listen(128)?;
        listener.set_nonblocking(true)?;

        let cancellation = CancellationToken::new();
        runtime.cancellation = Some(cancellation.clone());
        drop(runtime);

        let weak = Arc::downgrade(&self);
        let accept_cancellation = cancellation.clone();
        let accept = thread::Builder::new()
            .name(format!("glyco-accept-{}", self.node_id))
            .spawn(move || accept_loop(weak, listener, accept_cancellation))?;
        self.track_thread(accept);

        let weak = Arc::downgrade(&self);
        let discovery = thread::Builder::new()
            .name(format!("glyco-discovery-{}", self.node_id))
            .spawn(move || discovery_loop(weak, cancellation))?;
        self.track_thread(discovery);
        Ok(())
    }

    fn stop_blocking(&self) -> Result<()> {
        let cancellation = self
            .runtime
            .lock()
            .expect("runtime state poisoned")
            .cancellation
            .take();
        let Some(cancellation) = cancellation else {
            return Ok(());
        };

        cancellation.cancel();
        let peers: Vec<Arc<Peer>> = self
            .peers
            .write()
            .expect("peer map poisoned")
            .drain()
            .map(|(_, peer)| peer)
            .collect();
        for peer in peers {
            peer.shutdown();
        }

        loop {
            let handles = {
                let mut handles = self.threads.lock().expect("thread list poisoned");
                if handles.is_empty() {
                    break;
                }
                std::mem::take(&mut *handles)
            };
            for handle in handles {
                let _ = handle.join();
            }
        }

        match std::fs::remove_file(&self.socket_path) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(error.into()),
        }
    }

    fn cancel_without_joining(&self) {
        if let Some(cancellation) = self
            .runtime
            .lock()
            .expect("runtime state poisoned")
            .cancellation
            .take()
        {
            cancellation.cancel();
        }
        for peer in self
            .peers
            .write()
            .expect("peer map poisoned")
            .drain()
            .map(|(_, peer)| peer)
        {
            peer.shutdown();
        }
        let _ = std::fs::remove_file(&self.socket_path);
    }

    fn remove_stale_socket(&self) -> Result<()> {
        if !self.socket_path.exists() {
            return Ok(());
        }

        let probe = Socket::new(Domain::UNIX, Type::STREAM, None)?;
        match probe.connect(&SockAddr::unix(&self.socket_path)?) {
            Ok(()) => Err(GlycoError::NodeAlreadyRunning(self.node_id.clone())),
            Err(_) => {
                std::fs::remove_file(&self.socket_path)?;
                Ok(())
            }
        }
    }

    fn track_thread(&self, handle: JoinHandle<()>) {
        self.threads
            .lock()
            .expect("thread list poisoned")
            .push(handle);
    }

    fn spawn_incoming_handler(self: &Arc<Self>, socket: Socket, cancellation: CancellationToken) {
        let weak = Arc::downgrade(self);
        match thread::Builder::new()
            .name(format!("glyco-handshake-{}", self.node_id))
            .spawn(move || handle_incoming(weak, socket, cancellation))
        {
            Ok(handle) => self.track_thread(handle),
            Err(error) => {
                self.report_fault(None, format!("could not spawn handshake worker: {error}"))
            }
        }
    }

    fn connect_to_peer(self: &Arc<Self>, peer_id: &str, cancellation: &CancellationToken) {
        if cancellation.is_cancelled()
            || self
                .peers
                .read()
                .expect("peer map poisoned")
                .contains_key(peer_id)
        {
            return;
        }

        let peer_path = self.socket_dir.join(format!("{peer_id}.sock"));
        let socket = match Socket::new(Domain::UNIX, Type::STREAM, None) {
            Ok(socket) => socket,
            Err(error) => {
                self.report_fault(Some(peer_id.to_owned()), error.to_string());
                return;
            }
        };
        if socket
            .connect(&match SockAddr::unix(&peer_path) {
                Ok(address) => address,
                Err(error) => {
                    self.report_fault(Some(peer_id.to_owned()), error.to_string());
                    return;
                }
            })
            .is_err()
        {
            return;
        }
        let _ = socket.set_write_timeout(Some(WRITE_TIMEOUT));

        let handshake = match frame_message(self.node_id.as_bytes(), MAX_HANDSHAKE_LENGTH) {
            Ok(handshake) => handshake,
            Err(error) => {
                self.report_fault(Some(peer_id.to_owned()), error.to_string());
                return;
            }
        };
        let mut writable = &socket;
        if let Err(error) = writable.write_all(&handshake) {
            self.report_fault(
                Some(peer_id.to_owned()),
                format!("handshake failed: {error}"),
            );
            return;
        }

        if let Err(error) = self.install_peer(peer_id.to_owned(), socket, cancellation.clone()) {
            self.report_fault(Some(peer_id.to_owned()), error.to_string());
        }
    }

    fn install_peer(
        self: &Arc<Self>,
        peer_id: String,
        socket: Socket,
        cancellation: CancellationToken,
    ) -> Result<()> {
        validate_node_id(&peer_id)?;
        if peer_id == self.node_id {
            return Err(GlycoError::Protocol(
                "peer claimed the local node id".into(),
            ));
        }
        socket.set_nonblocking(false)?;
        socket.set_read_timeout(None)?;
        socket.set_write_timeout(Some(WRITE_TIMEOUT))?;

        let reader_socket = socket.try_clone()?;
        let control_socket = socket.try_clone()?;
        let generation = self.next_generation.fetch_add(1, AtomicOrdering::Relaxed);
        let (writer, receiver) = mpsc::channel();
        let peer = Arc::new(Peer {
            id: peer_id.clone(),
            generation,
            writer: Mutex::new(Some(writer)),
            control_socket,
            closed: AtomicBool::new(false),
        });

        {
            let mut peers = self.peers.write().expect("peer map poisoned");
            if peers.contains_key(&peer_id) {
                let _ = socket.shutdown(Shutdown::Both);
                return Ok(());
            }
            peers.insert(peer_id.clone(), peer.clone());
        }

        let weak = Arc::downgrade(self);
        let writer_peer_id = peer_id.clone();
        let writer_cancellation = cancellation.clone();
        let writer_handle = thread::Builder::new()
            .name(format!("glyco-write-{}-{peer_id}", self.node_id))
            .spawn(move || {
                writer_loop(
                    weak,
                    writer_peer_id,
                    generation,
                    socket,
                    receiver,
                    writer_cancellation,
                )
            })?;
        self.track_thread(writer_handle);

        let weak = Arc::downgrade(self);
        let reader_handle = thread::Builder::new()
            .name(format!("glyco-read-{}-{peer_id}", self.node_id))
            .spawn(move || reader_loop(weak, peer_id, generation, reader_socket, cancellation))?;
        self.track_thread(reader_handle);

        let _ = self
            .events
            .send(ConnexonEvent::PeerConnected(peer.id.clone()));
        Ok(())
    }

    async fn write_to_peer(&self, peer: Arc<Peer>, frame: Arc<[u8]>) -> Result<()> {
        let acknowledgement = peer.enqueue(frame)?;
        match acknowledgement.await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(error)) => {
                self.disconnect_peer(&peer.id, peer.generation);
                Err(error.into())
            }
            Err(_) => {
                self.disconnect_peer(&peer.id, peer.generation);
                Err(broken_pipe("peer writer stopped").into())
            }
        }
    }

    fn disconnect_peer(&self, peer_id: &str, generation: u64) {
        let removed = {
            let mut peers = self.peers.write().expect("peer map poisoned");
            match peers.get(peer_id) {
                Some(peer) if peer.generation == generation => peers.remove(peer_id),
                _ => None,
            }
        };
        if let Some(peer) = removed {
            peer.shutdown();
            let _ = self
                .events
                .send(ConnexonEvent::PeerDisconnected(peer_id.to_owned()));
        }
    }

    fn report_fault(&self, peer: Option<String>, message: String) {
        tracing::warn!(peer = peer.as_deref(), "{message}");
        let _ = self.events.send(ConnexonEvent::Fault { peer, message });
    }
}

impl Peer {
    fn enqueue(&self, data: Arc<[u8]>) -> io::Result<oneshot::Receiver<io::Result<()>>> {
        let (acknowledgement, receiver) = oneshot::channel();
        let writer = self
            .writer
            .lock()
            .expect("peer writer poisoned")
            .clone()
            .ok_or_else(|| broken_pipe("peer is closed"))?;
        writer
            .send(WriteRequest {
                data,
                acknowledgement,
            })
            .map_err(|_| broken_pipe("peer writer stopped"))?;
        Ok(receiver)
    }

    fn shutdown(&self) {
        if self.closed.swap(true, AtomicOrdering::AcqRel) {
            return;
        }
        self.writer.lock().expect("peer writer poisoned").take();
        let _ = self.control_socket.shutdown(Shutdown::Both);
    }
}

fn accept_loop(inner: Weak<Inner>, listener: Socket, cancellation: CancellationToken) {
    while !cancellation.is_cancelled() {
        match listener.accept() {
            Ok((socket, _)) => {
                if let Some(inner) = inner.upgrade() {
                    let _ = socket.set_nonblocking(false);
                    inner.spawn_incoming_handler(socket, cancellation.clone());
                } else {
                    break;
                }
            }
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                thread::sleep(ACCEPT_POLL_INTERVAL);
            }
            Err(error) => {
                if let Some(inner) = inner.upgrade()
                    && !cancellation.is_cancelled()
                {
                    inner.report_fault(None, format!("accept failed: {error}"));
                }
                if !cancellation.is_cancelled() {
                    thread::sleep(ACCEPT_POLL_INTERVAL);
                }
            }
        }
    }
}

fn handle_incoming(inner: Weak<Inner>, socket: Socket, cancellation: CancellationToken) {
    let Some(inner) = inner.upgrade() else {
        return;
    };
    let _ = socket.set_read_timeout(Some(HANDSHAKE_TIMEOUT));
    let handshake = match read_message(&socket, MAX_HANDSHAKE_LENGTH) {
        Ok(handshake) => handshake,
        Err(error) => {
            inner.report_fault(None, format!("invalid peer handshake: {error}"));
            return;
        }
    };
    let peer_id = match String::from_utf8(handshake) {
        Ok(peer_id) => peer_id,
        Err(error) => {
            inner.report_fault(None, format!("peer id is not UTF-8: {error}"));
            return;
        }
    };
    if let Err(error) = inner.install_peer(peer_id.clone(), socket, cancellation) {
        inner.report_fault(Some(peer_id), error.to_string());
    }
}

fn discovery_loop(inner: Weak<Inner>, cancellation: CancellationToken) {
    while !cancellation.is_cancelled() {
        let Some(inner) = inner.upgrade() else {
            return;
        };
        scan_and_connect(&inner, &cancellation);
        let interval = inner.discovery_interval;
        drop(inner);
        sleep_interruptibly(&cancellation, interval);
    }
}

fn scan_and_connect(inner: &Arc<Inner>, cancellation: &CancellationToken) {
    let entries = match std::fs::read_dir(&inner.socket_dir) {
        Ok(entries) => entries,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return,
        Err(error) => {
            inner.report_fault(None, format!("socket discovery failed: {error}"));
            return;
        }
    };

    for entry in entries.flatten() {
        if cancellation.is_cancelled() {
            return;
        }
        let path = entry.path();
        if path.extension() != Some(OsStr::new("sock")) {
            continue;
        }
        let Some(peer_id) = path.file_stem().and_then(OsStr::to_str) else {
            continue;
        };
        if peer_id == inner.node_id
            || dotnet_ordinal_cmp(&inner.node_id, peer_id) == Ordering::Greater
        {
            continue;
        }
        if validate_node_id(peer_id).is_err() {
            continue;
        }
        inner.connect_to_peer(peer_id, cancellation);
    }
}

fn writer_loop(
    inner: Weak<Inner>,
    peer_id: String,
    generation: u64,
    socket: Socket,
    receiver: mpsc::Receiver<WriteRequest>,
    cancellation: CancellationToken,
) {
    let mut writable = &socket;
    while !cancellation.is_cancelled() {
        let request = match receiver.recv_timeout(INTERRUPT_POLL_INTERVAL) {
            Ok(request) => request,
            Err(mpsc::RecvTimeoutError::Timeout) => continue,
            Err(mpsc::RecvTimeoutError::Disconnected) => break,
        };

        match writable.write_all(&request.data) {
            Ok(()) => {
                let _ = request.acknowledgement.send(Ok(()));
            }
            Err(error) => {
                let kind = error.kind();
                let message = error.to_string();
                let _ = request
                    .acknowledgement
                    .send(Err(io::Error::new(kind, message.clone())));
                if let Some(inner) = inner.upgrade() {
                    inner.report_fault(Some(peer_id.clone()), format!("write failed: {message}"));
                    inner.disconnect_peer(&peer_id, generation);
                }
                break;
            }
        }
    }

    for request in receiver.try_iter() {
        let _ = request
            .acknowledgement
            .send(Err(broken_pipe("peer writer stopped")));
    }
}

fn reader_loop(
    inner: Weak<Inner>,
    peer_id: String,
    generation: u64,
    socket: Socket,
    cancellation: CancellationToken,
) {
    while !cancellation.is_cancelled() {
        let Some(inner) = inner.upgrade() else {
            return;
        };
        match read_message(&socket, inner.max_frame_length) {
            Ok(bytes) => match Glycosyl::from_bytes(&bytes) {
                Ok(message) => {
                    let _ = inner.events.send(ConnexonEvent::Message(message));
                }
                Err(error) => {
                    inner.report_fault(
                        Some(peer_id.clone()),
                        format!("invalid JSON message ignored: {error}"),
                    );
                }
            },
            Err(error) => {
                if !cancellation.is_cancelled() {
                    inner.report_fault(Some(peer_id.clone()), format!("read failed: {error}"));
                }
                inner.disconnect_peer(&peer_id, generation);
                return;
            }
        }
    }
}

fn frame_message(payload: &[u8], max_frame_length: usize) -> Result<Vec<u8>> {
    if payload.len() > max_frame_length || payload.len() > i32::MAX as usize {
        return Err(GlycoError::FrameTooLarge {
            size: payload.len(),
            limit: max_frame_length.min(i32::MAX as usize),
        });
    }
    let mut framed = Vec::with_capacity(4 + payload.len());
    framed.extend_from_slice(&(payload.len() as i32).to_le_bytes());
    framed.extend_from_slice(payload);
    Ok(framed)
}

fn read_message(socket: &Socket, max_frame_length: usize) -> io::Result<Vec<u8>> {
    let mut readable = socket;
    let mut length = [0_u8; 4];
    readable.read_exact(&mut length)?;
    let length = i32::from_le_bytes(length);
    if length < 0 || length as usize > max_frame_length {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("invalid message length: {length}"),
        ));
    }
    let mut payload = vec![0; length as usize];
    readable.read_exact(&mut payload)?;
    Ok(payload)
}

fn validate_node_id(node_id: &str) -> Result<()> {
    let invalid_windows_character = |character: char| {
        matches!(
            character,
            '<' | '>' | ':' | '"' | '/' | '\\' | '|' | '?' | '*'
        )
    };
    if node_id.is_empty()
        || matches!(node_id, "." | "..")
        || node_id.chars().any(|character| {
            character == '\0' || character.is_control() || invalid_windows_character(character)
        })
        || Path::new(node_id).file_name() != Some(OsStr::new(node_id))
    {
        return Err(GlycoError::InvalidNodeId(node_id.to_owned()));
    }
    Ok(())
}

fn dotnet_ordinal_cmp(left: &str, right: &str) -> Ordering {
    left.encode_utf16().cmp(right.encode_utf16())
}

fn sleep_interruptibly(cancellation: &CancellationToken, duration: Duration) {
    let started = std::time::Instant::now();
    while !cancellation.is_cancelled() {
        let elapsed = started.elapsed();
        if elapsed >= duration {
            break;
        }
        thread::sleep((duration - elapsed).min(INTERRUPT_POLL_INTERVAL));
    }
}

fn broken_pipe(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::BrokenPipe, message)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn frame_is_dotnet_little_endian_length_prefixed() {
        let framed = frame_message(b"hello", 100).unwrap();
        assert_eq!(&framed[..4], &[5, 0, 0, 0]);
        assert_eq!(&framed[4..], b"hello");
    }

    #[test]
    fn rejects_unsafe_node_ids() {
        for id in ["", ".", "..", "a/b", "a\\b", "a:b", "bad\nname"] {
            assert!(validate_node_id(id).is_err(), "{id:?} should be rejected");
        }
        assert!(validate_node_id("节点-alpha_1.2").is_ok());
    }

    #[test]
    fn ordinal_comparison_matches_ascii_and_utf16_order() {
        assert_eq!(dotnet_ordinal_cmp("alpha", "beta"), Ordering::Less);
        assert_eq!(dotnet_ordinal_cmp("beta", "alpha"), Ordering::Greater);
        assert_eq!(dotnet_ordinal_cmp("same", "same"), Ordering::Equal);
    }
}
