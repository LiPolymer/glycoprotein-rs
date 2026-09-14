use std::any::TypeId;
use std::collections::{BTreeMap, HashMap};
use std::future::{Future, pending};
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};

use schemars::JsonSchema;
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::Value;
use tokio::sync::{Semaphore, broadcast, oneshot, watch};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::transport::{Connexon, ConnexonEvent, UnixDomainMeshConnexon};
use crate::{
    Beacon, Event, EventField, Field, GlycoError, Glycosyl, HandlerError, Heartbeat, MethodField,
    Query, RemoteError, Reply, Result, generate_schema, validate_json,
};

const DEFAULT_QUERY_TIMEOUT: Duration = Duration::from_secs(30);
const DEFAULT_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(1);
const DEFAULT_FULL_BEACON_TICKS: u32 = 5;
const DEFAULT_PRESENTER_EXPIRY: Duration = Duration::from_secs(3);
const DEFAULT_CLEANUP_INTERVAL: Duration = Duration::from_secs(1);
const DEFAULT_MAX_CONCURRENT_HANDLERS: usize = 64;

type HandlerFuture =
    Pin<Box<dyn Future<Output = std::result::Result<Option<Value>, HandlerError>> + Send>>;
type RawMethodHandler = Arc<dyn Fn(Option<Value>, RequestContext) -> HandlerFuture + Send + Sync>;

type EventFuture = Pin<Box<dyn Future<Output = std::result::Result<(), HandlerError>> + Send>>;
type RawEventHandler = Arc<dyn Fn(Option<Value>) -> EventFuture + Send + Sync>;

type PendingReply = std::result::Result<Option<Value>, RemoteError>;

#[derive(Debug, Clone)]
pub struct RequestContext {
    pub qid: Uuid,
    pub source_gid: Option<String>,
    pub cancellation: CancellationToken,
}

#[derive(Debug, Clone)]
pub struct CallOptions {
    pub timeout: Option<Duration>,
    pub cancellation: CancellationToken,
}

impl Default for CallOptions {
    fn default() -> Self {
        Self {
            timeout: Some(DEFAULT_QUERY_TIMEOUT),
            cancellation: CancellationToken::new(),
        }
    }
}

impl CallOptions {
    pub fn with_timeout(mut self, timeout: Option<Duration>) -> Self {
        self.timeout = timeout;
        self
    }

    pub fn with_cancellation(mut self, cancellation: CancellationToken) -> Self {
        self.cancellation = cancellation;
        self
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum PresenterEvent {
    Discovered(Beacon),
    Changed { previous: Beacon, current: Beacon },
    Expired(Beacon),
}

pub struct GlycoComplexBuilder {
    id: String,
    vendor: Option<String>,
    connexon: Option<Arc<dyn Connexon>>,
    query_timeout: Option<Duration>,
    loopback_presenter: bool,
    max_concurrent_handlers: usize,
}

impl GlycoComplexBuilder {
    pub fn new(id: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            vendor: None,
            connexon: None,
            query_timeout: Some(DEFAULT_QUERY_TIMEOUT),
            loopback_presenter: true,
            max_concurrent_handlers: DEFAULT_MAX_CONCURRENT_HANDLERS,
        }
    }

    pub fn vendor(mut self, vendor: impl Into<String>) -> Self {
        self.vendor = Some(vendor.into());
        self
    }

    pub fn connexon<C>(mut self, connexon: C) -> Self
    where
        C: Connexon + 'static,
    {
        self.connexon = Some(Arc::new(connexon));
        self
    }

    pub fn shared_connexon(mut self, connexon: Arc<dyn Connexon>) -> Self {
        self.connexon = Some(connexon);
        self
    }

    pub fn query_timeout(mut self, timeout: Option<Duration>) -> Self {
        self.query_timeout = timeout;
        self
    }

    pub fn loopback_presenter(mut self, enabled: bool) -> Self {
        self.loopback_presenter = enabled;
        self
    }

    pub fn max_concurrent_handlers(mut self, maximum: usize) -> Self {
        self.max_concurrent_handlers = maximum.max(1);
        self
    }

    pub fn build(self) -> Result<GlycoComplex> {
        let connexon = match self.connexon {
            Some(connexon) => connexon,
            None => Arc::new(UnixDomainMeshConnexon::new(self.id.clone())?),
        };
        if connexon.node_id() != self.id {
            return Err(GlycoError::InvalidNodeId(format!(
                "builder id '{}' does not match connexon id '{}'",
                self.id,
                connexon.node_id()
            )));
        }
        let (presenter_events, _) = broadcast::channel(256);
        let (beacon_revision, _) = watch::channel(0_u64);

        Ok(GlycoComplex {
            inner: Arc::new(NodeInner {
                id: self.id,
                vendor: RwLock::new(self.vendor),
                connexon,
                methods: RwLock::new(BTreeMap::new()),
                emitted_events: RwLock::new(BTreeMap::new()),
                event_handlers: RwLock::new(HashMap::new()),
                presenters: RwLock::new(BTreeMap::new()),
                pending: Mutex::new(HashMap::new()),
                lifecycle: tokio::sync::Mutex::new(Lifecycle::default()),
                active_cancellation: Mutex::new(None),
                started: AtomicBool::new(false),
                query_timeout: RwLock::new(self.query_timeout),
                loopback_presenter: AtomicBool::new(self.loopback_presenter),
                handler_semaphore: Arc::new(Semaphore::new(self.max_concurrent_handlers)),
                presenter_events,
                beacon_revision,
                heartbeat_interval: DEFAULT_HEARTBEAT_INTERVAL,
                full_beacon_ticks: DEFAULT_FULL_BEACON_TICKS,
                presenter_expiry: DEFAULT_PRESENTER_EXPIRY,
                cleanup_interval: DEFAULT_CLEANUP_INTERVAL,
            }),
        })
    }
}

#[derive(Clone)]
pub struct GlycoComplex {
    inner: Arc<NodeInner>,
}

struct NodeInner {
    id: String,
    vendor: RwLock<Option<String>>,
    connexon: Arc<dyn Connexon>,
    methods: RwLock<BTreeMap<String, RegisteredMethod>>,
    emitted_events: RwLock<BTreeMap<String, RegisteredEvent>>,
    event_handlers: RwLock<HashMap<(String, String), RawEventHandler>>,
    presenters: RwLock<BTreeMap<String, PresenterRecord>>,
    pending: Mutex<HashMap<Uuid, oneshot::Sender<PendingReply>>>,
    lifecycle: tokio::sync::Mutex<Lifecycle>,
    active_cancellation: Mutex<Option<CancellationToken>>,
    started: AtomicBool,
    query_timeout: RwLock<Option<Duration>>,
    loopback_presenter: AtomicBool,
    handler_semaphore: Arc<Semaphore>,
    presenter_events: broadcast::Sender<PresenterEvent>,
    beacon_revision: watch::Sender<u64>,
    heartbeat_interval: Duration,
    full_beacon_ticks: u32,
    presenter_expiry: Duration,
    cleanup_interval: Duration,
}

#[derive(Default)]
struct Lifecycle {
    tasks: Vec<JoinHandle<()>>,
}

struct RegisteredMethod {
    field: MethodField,
    handler: RawMethodHandler,
}

struct RegisteredEvent {
    field: EventField,
    argument_type: Option<TypeId>,
}

struct PresenterRecord {
    beacon: Beacon,
    signature: String,
    last_seen: Instant,
}

impl GlycoComplex {
    pub fn builder(id: impl Into<String>) -> GlycoComplexBuilder {
        GlycoComplexBuilder::new(id)
    }

    pub fn new(id: impl Into<String>) -> Result<Self> {
        Self::builder(id).build()
    }

    pub fn id(&self) -> &str {
        &self.inner.id
    }

    pub fn connexon(&self) -> Arc<dyn Connexon> {
        self.inner.connexon.clone()
    }

    pub fn is_started(&self) -> bool {
        self.inner.started.load(Ordering::Acquire)
    }

    pub fn vendor(&self) -> Option<String> {
        self.inner
            .vendor
            .read()
            .expect("vendor lock poisoned")
            .clone()
    }

    pub fn set_vendor(&self, vendor: Option<String>) {
        *self.inner.vendor.write().expect("vendor lock poisoned") = vendor;
        self.inner.mark_beacon_changed();
    }

    pub fn query_timeout(&self) -> Option<Duration> {
        *self
            .inner
            .query_timeout
            .read()
            .expect("query timeout lock poisoned")
    }

    pub fn set_query_timeout(&self, timeout: Option<Duration>) {
        *self
            .inner
            .query_timeout
            .write()
            .expect("query timeout lock poisoned") = timeout;
    }

    pub fn loopback_presenter(&self) -> bool {
        self.inner.loopback_presenter.load(Ordering::Acquire)
    }

    pub fn set_loopback_presenter(&self, enabled: bool) {
        self.inner
            .loopback_presenter
            .store(enabled, Ordering::Release);
    }

    pub fn presenters(&self) -> Vec<Beacon> {
        self.inner
            .presenters
            .read()
            .expect("presenter map poisoned")
            .values()
            .map(|record| record.beacon.clone())
            .collect()
    }

    pub fn subscribe_presenters(&self) -> broadcast::Receiver<PresenterEvent> {
        self.inner.presenter_events.subscribe()
    }

    pub fn register_raw_function<F, Fut>(&self, field: MethodField, handler: F) -> Result<()>
    where
        F: Fn(Option<Value>, RequestContext) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = std::result::Result<Option<Value>, HandlerError>> + Send + 'static,
    {
        let handler = Arc::new(handler);
        let raw: RawMethodHandler = Arc::new(move |payload, context| {
            let handler = handler.clone();
            Box::pin(async move { handler(payload, context).await })
        });
        self.inner.insert_method(field, raw)
    }

    pub fn register_raw_function_sync<F>(&self, field: MethodField, handler: F) -> Result<()>
    where
        F: Fn(Option<Value>, RequestContext) -> std::result::Result<Option<Value>, HandlerError>
            + Send
            + Sync
            + 'static,
    {
        let handler = Arc::new(handler);
        self.register_raw_function(field, move |payload, context| {
            let handler = handler.clone();
            async move {
                tokio::task::spawn_blocking(move || handler(payload, context))
                    .await
                    .map_err(|error| HandlerError::new(error.to_string()))?
            }
        })
    }

    pub fn register_function<Req, Res, F, Fut>(
        &self,
        mut field: MethodField,
        handler: F,
    ) -> Result<()>
    where
        Req: DeserializeOwned + JsonSchema + Send + 'static,
        Res: Serialize + JsonSchema + Send + 'static,
        F: Fn(Req, RequestContext) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = std::result::Result<Res, HandlerError>> + Send + 'static,
    {
        if field.query_schema.is_none() {
            field.query_schema = Some(generate_schema::<Req>()?);
        }
        if field.receipt_schema.is_none() {
            field.receipt_schema = Some(generate_schema::<Res>()?);
        }
        let handler = Arc::new(handler);
        self.register_raw_function(field, move |payload, context| {
            let handler = handler.clone();
            async move {
                let payload = payload.unwrap_or(Value::Null);
                let request = serde_json::from_value(payload).map_err(|error| {
                    HandlerError::new(format!("request JSON is invalid: {error}"))
                })?;
                let response = handler(request, context).await?;
                serde_json::to_value(response)
                    .map(Some)
                    .map_err(|error| HandlerError::new(format!("response JSON failed: {error}")))
            }
        })
    }

    pub fn register_function_sync<Req, Res, F>(
        &self,
        mut field: MethodField,
        handler: F,
    ) -> Result<()>
    where
        Req: DeserializeOwned + JsonSchema + Send + 'static,
        Res: Serialize + JsonSchema + Send + 'static,
        F: Fn(Req, RequestContext) -> std::result::Result<Res, HandlerError>
            + Send
            + Sync
            + 'static,
    {
        if field.query_schema.is_none() {
            field.query_schema = Some(generate_schema::<Req>()?);
        }
        if field.receipt_schema.is_none() {
            field.receipt_schema = Some(generate_schema::<Res>()?);
        }
        let handler = Arc::new(handler);
        self.register_raw_function_sync(field, move |payload, context| {
            let request = serde_json::from_value(payload.unwrap_or(Value::Null))
                .map_err(|error| HandlerError::new(format!("request JSON is invalid: {error}")))?;
            let response = handler(request, context)?;
            serde_json::to_value(response)
                .map(Some)
                .map_err(|error| HandlerError::new(format!("response JSON failed: {error}")))
        })
    }

    pub fn register_query<Res, F, Fut>(&self, mut field: MethodField, handler: F) -> Result<()>
    where
        Res: Serialize + JsonSchema + Send + 'static,
        F: Fn(RequestContext) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = std::result::Result<Res, HandlerError>> + Send + 'static,
    {
        field.query_schema = None;
        if field.receipt_schema.is_none() {
            field.receipt_schema = Some(generate_schema::<Res>()?);
        }
        let handler = Arc::new(handler);
        self.register_raw_function(field, move |_, context| {
            let handler = handler.clone();
            async move {
                let response = handler(context).await?;
                serde_json::to_value(response)
                    .map(Some)
                    .map_err(|error| HandlerError::new(format!("response JSON failed: {error}")))
            }
        })
    }

    pub fn register_query_sync<Res, F>(&self, mut field: MethodField, handler: F) -> Result<()>
    where
        Res: Serialize + JsonSchema + Send + 'static,
        F: Fn(RequestContext) -> std::result::Result<Res, HandlerError> + Send + Sync + 'static,
    {
        field.query_schema = None;
        if field.receipt_schema.is_none() {
            field.receipt_schema = Some(generate_schema::<Res>()?);
        }
        let handler = Arc::new(handler);
        self.register_raw_function_sync(field, move |_, context| {
            let response = handler(context)?;
            serde_json::to_value(response)
                .map(Some)
                .map_err(|error| HandlerError::new(format!("response JSON failed: {error}")))
        })
    }

    pub fn register_action<F, Fut>(&self, mut field: MethodField, handler: F) -> Result<()>
    where
        F: Fn(RequestContext) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = std::result::Result<(), HandlerError>> + Send + 'static,
    {
        field.query_schema = None;
        field.receipt_schema = None;
        let handler = Arc::new(handler);
        self.register_raw_function(field, move |_, context| {
            let handler = handler.clone();
            async move {
                handler(context).await?;
                Ok(None)
            }
        })
    }

    pub fn register_action_sync<F>(&self, mut field: MethodField, handler: F) -> Result<()>
    where
        F: Fn(RequestContext) -> std::result::Result<(), HandlerError> + Send + Sync + 'static,
    {
        field.query_schema = None;
        field.receipt_schema = None;
        let handler = Arc::new(handler);
        self.register_raw_function_sync(field, move |_, context| {
            handler(context)?;
            Ok(None)
        })
    }

    pub fn register_action_with<Req, F, Fut>(
        &self,
        mut field: MethodField,
        handler: F,
    ) -> Result<()>
    where
        Req: DeserializeOwned + JsonSchema + Send + 'static,
        F: Fn(Req, RequestContext) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = std::result::Result<(), HandlerError>> + Send + 'static,
    {
        if field.query_schema.is_none() {
            field.query_schema = Some(generate_schema::<Req>()?);
        }
        field.receipt_schema = None;
        let handler = Arc::new(handler);
        self.register_raw_function(field, move |payload, context| {
            let handler = handler.clone();
            async move {
                let request =
                    serde_json::from_value(payload.unwrap_or(Value::Null)).map_err(|error| {
                        HandlerError::new(format!("request JSON is invalid: {error}"))
                    })?;
                handler(request, context).await?;
                Ok(None)
            }
        })
    }

    pub fn register_action_with_sync<Req, F>(
        &self,
        mut field: MethodField,
        handler: F,
    ) -> Result<()>
    where
        Req: DeserializeOwned + JsonSchema + Send + 'static,
        F: Fn(Req, RequestContext) -> std::result::Result<(), HandlerError> + Send + Sync + 'static,
    {
        if field.query_schema.is_none() {
            field.query_schema = Some(generate_schema::<Req>()?);
        }
        field.receipt_schema = None;
        let handler = Arc::new(handler);
        self.register_raw_function_sync(field, move |payload, context| {
            let request = serde_json::from_value(payload.unwrap_or(Value::Null))
                .map_err(|error| HandlerError::new(format!("request JSON is invalid: {error}")))?;
            handler(request, context)?;
            Ok(None)
        })
    }

    pub fn register_event<T>(&self, mut field: EventField) -> Result<()>
    where
        T: JsonSchema + 'static,
    {
        if field.call_arg_schema.is_none() {
            field.call_arg_schema = Some(generate_schema::<T>()?);
        }
        self.inner.insert_event(field, Some(TypeId::of::<T>()))
    }

    pub fn register_signal(&self, mut field: EventField) -> Result<()> {
        field.call_arg_schema = None;
        self.inner.insert_event(field, None)
    }

    pub fn on_event_raw<F, Fut>(&self, gid: impl Into<String>, fid: impl Into<String>, handler: F)
    where
        F: Fn(Option<Value>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = std::result::Result<(), HandlerError>> + Send + 'static,
    {
        let handler = Arc::new(handler);
        let raw: RawEventHandler = Arc::new(move |argument| {
            let handler = handler.clone();
            Box::pin(async move { handler(argument).await })
        });
        self.inner
            .event_handlers
            .write()
            .expect("event handler map poisoned")
            .insert((gid.into(), fid.into()), raw);
    }

    pub fn on_event<T, F, Fut>(&self, gid: impl Into<String>, fid: impl Into<String>, handler: F)
    where
        T: DeserializeOwned + Send + 'static,
        F: Fn(T) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = std::result::Result<(), HandlerError>> + Send + 'static,
    {
        let handler = Arc::new(handler);
        self.on_event_raw(gid, fid, move |argument| {
            let handler = handler.clone();
            async move {
                let argument =
                    serde_json::from_value(argument.unwrap_or(Value::Null)).map_err(|error| {
                        HandlerError::new(format!("event JSON is invalid: {error}"))
                    })?;
                handler(argument).await
            }
        });
    }

    pub fn on_event_sync<T, F>(&self, gid: impl Into<String>, fid: impl Into<String>, handler: F)
    where
        T: DeserializeOwned + Send + 'static,
        F: Fn(T) -> std::result::Result<(), HandlerError> + Send + Sync + 'static,
    {
        let handler = Arc::new(handler);
        self.on_event(gid, fid, move |argument| {
            let handler = handler.clone();
            async move {
                tokio::task::spawn_blocking(move || handler(argument))
                    .await
                    .map_err(|error| HandlerError::new(error.to_string()))?
            }
        });
    }

    pub fn remove_event_handler(&self, gid: &str, fid: &str) -> bool {
        self.inner
            .event_handlers
            .write()
            .expect("event handler map poisoned")
            .remove(&(gid.to_owned(), fid.to_owned()))
            .is_some()
    }

    pub fn remove_field(&self, fid: &str) -> bool {
        let method_removed = self
            .inner
            .methods
            .write()
            .expect("method map poisoned")
            .remove(fid)
            .is_some();
        let event_removed = self
            .inner
            .emitted_events
            .write()
            .expect("event map poisoned")
            .remove(fid)
            .is_some();
        if method_removed || event_removed {
            self.inner.mark_beacon_changed();
            true
        } else {
            false
        }
    }

    pub fn refresh_beacon(&self) {
        self.inner.mark_beacon_changed();
    }

    pub async fn start(&self) -> Result<()> {
        let mut lifecycle = self.inner.lifecycle.lock().await;
        if self.inner.started.load(Ordering::Acquire) {
            return Ok(());
        }

        let receiver = self.inner.connexon.subscribe();
        self.inner.connexon.start().await?;
        let cancellation = CancellationToken::new();
        *self
            .inner
            .active_cancellation
            .lock()
            .expect("active cancellation lock poisoned") = Some(cancellation.clone());

        let dispatch = tokio::spawn(dispatch_loop(
            self.inner.clone(),
            receiver,
            cancellation.clone(),
        ));
        let cleanup = tokio::spawn(cleanup_loop(self.inner.clone(), cancellation.clone()));

        self.inner.started.store(true, Ordering::Release);
        if let Err(error) = self.inner.send_full_beacon().await {
            self.inner.started.store(false, Ordering::Release);
            cancellation.cancel();
            let _ = self.inner.connexon.stop().await;
            let _ = dispatch.await;
            let _ = cleanup.await;
            self.inner
                .active_cancellation
                .lock()
                .expect("active cancellation lock poisoned")
                .take();
            return Err(error);
        }

        let beacon = tokio::spawn(beacon_loop(
            self.inner.clone(),
            self.inner.beacon_revision.subscribe(),
            cancellation,
        ));
        lifecycle.tasks = vec![dispatch, cleanup, beacon];
        Ok(())
    }

    pub async fn stop(&self) -> Result<()> {
        let mut lifecycle = self.inner.lifecycle.lock().await;
        if !self.inner.started.swap(false, Ordering::AcqRel) {
            return self.inner.connexon.stop().await;
        }

        if let Some(cancellation) = self
            .inner
            .active_cancellation
            .lock()
            .expect("active cancellation lock poisoned")
            .take()
        {
            cancellation.cancel();
        }
        let transport_result = self.inner.connexon.stop().await;
        for task in lifecycle.tasks.drain(..) {
            if let Err(error) = task.await {
                tracing::debug!("node task stopped with error: {error}");
            }
        }
        self.inner
            .presenters
            .write()
            .expect("presenter map poisoned")
            .clear();
        self.inner
            .pending
            .lock()
            .expect("pending map poisoned")
            .clear();
        transport_result
    }

    pub async fn call_raw(
        &self,
        gid: &str,
        fid: &str,
        payload: Option<Value>,
    ) -> Result<Option<Value>> {
        let options = CallOptions {
            timeout: self.query_timeout(),
            ..CallOptions::default()
        };
        self.call_raw_with_options(gid, fid, payload, options).await
    }

    pub async fn call_raw_with_options(
        &self,
        gid: &str,
        fid: &str,
        payload: Option<Value>,
        options: CallOptions,
    ) -> Result<Option<Value>> {
        if !self.is_started() {
            return Err(GlycoError::NotStarted);
        }
        let method = self.inner.find_remote_method(gid, fid)?;
        if let (Some(payload), Some(schema)) = (&payload, &method.query_schema) {
            validate_json(schema, payload)?;
        }

        let query = Query::new(gid, &self.inner.id, fid, payload);
        let qid = query.qid;
        let (sender, receiver) = oneshot::channel();
        self.inner
            .pending
            .lock()
            .expect("pending map poisoned")
            .insert(qid, sender);

        if let Err(error) = self.inner.connexon.send(&Glycosyl::Query(query)).await {
            self.inner
                .pending
                .lock()
                .expect("pending map poisoned")
                .remove(&qid);
            return Err(error);
        }

        let timeout = async {
            match options.timeout {
                Some(timeout) => tokio::time::sleep(timeout).await,
                None => pending::<()>().await,
            }
        };
        tokio::pin!(timeout);

        let result = tokio::select! {
            reply = receiver => match reply {
                Ok(Ok(payload)) => Ok(payload),
                Ok(Err(error)) => Err(GlycoError::Remote(error)),
                Err(_) => Err(GlycoError::Task("reply channel closed".into())),
            },
            () = &mut timeout => Err(GlycoError::Timeout {
                qid,
                node: gid.to_owned(),
                field: fid.to_owned(),
            }),
            () = options.cancellation.cancelled() => Err(GlycoError::Cancelled(qid)),
        };

        self.inner
            .pending
            .lock()
            .expect("pending map poisoned")
            .remove(&qid);
        result
    }

    pub async fn call<Req, Res>(&self, gid: &str, fid: &str, request: &Req) -> Result<Option<Res>>
    where
        Req: Serialize + ?Sized,
        Res: DeserializeOwned,
    {
        let payload = serde_json::to_value(request).map_err(|source| GlycoError::TypedJson {
            context: "request",
            source,
        })?;
        self.call_raw(gid, fid, Some(payload))
            .await?
            .map(serde_json::from_value)
            .transpose()
            .map_err(|source| GlycoError::TypedJson {
                context: "response",
                source,
            })
    }

    pub async fn call_query<Res>(&self, gid: &str, fid: &str) -> Result<Option<Res>>
    where
        Res: DeserializeOwned,
    {
        self.call_raw(gid, fid, None)
            .await?
            .map(serde_json::from_value)
            .transpose()
            .map_err(|source| GlycoError::TypedJson {
                context: "response",
                source,
            })
    }

    pub async fn do_action(&self, gid: &str, fid: &str) -> Result<()> {
        self.call_raw(gid, fid, None).await.map(|_| ())
    }

    pub async fn do_action_with<T>(&self, gid: &str, fid: &str, argument: &T) -> Result<()>
    where
        T: Serialize + ?Sized,
    {
        let argument = serde_json::to_value(argument).map_err(|source| GlycoError::TypedJson {
            context: "action argument",
            source,
        })?;
        self.call_raw(gid, fid, Some(argument)).await.map(|_| ())
    }

    pub async fn emit_raw(&self, fid: &str, argument: Option<Value>) -> Result<()> {
        if !self.is_started() {
            return Err(GlycoError::NotStarted);
        }
        self.inner
            .connexon
            .send(&Glycosyl::Event(Event {
                gid: self.inner.id.clone(),
                fid: fid.to_owned(),
                arg: argument,
            }))
            .await
    }

    pub async fn emit_signal(&self, fid: &str) -> Result<()> {
        {
            let events = self
                .inner
                .emitted_events
                .read()
                .expect("event map poisoned");
            let event = events.get(fid).ok_or_else(|| GlycoError::FieldNotFound {
                node: self.inner.id.clone(),
                field: fid.to_owned(),
            })?;
            if event.argument_type.is_some() {
                return Err(GlycoError::Protocol(format!(
                    "event '{fid}' expects an argument"
                )));
            }
        }
        self.emit_raw(fid, None).await
    }

    pub async fn emit<T>(&self, fid: &str, argument: &T) -> Result<()>
    where
        T: Serialize + 'static,
    {
        {
            let events = self
                .inner
                .emitted_events
                .read()
                .expect("event map poisoned");
            let event = events.get(fid).ok_or_else(|| GlycoError::FieldNotFound {
                node: self.inner.id.clone(),
                field: fid.to_owned(),
            })?;
            if event.argument_type != Some(TypeId::of::<T>()) {
                return Err(GlycoError::Protocol(format!(
                    "event '{fid}' argument type does not match its registration"
                )));
            }
        }
        let argument = serde_json::to_value(argument).map_err(|source| GlycoError::TypedJson {
            context: "event argument",
            source,
        })?;
        self.emit_raw(fid, Some(argument)).await
    }
}

impl NodeInner {
    fn insert_method(&self, field: MethodField, handler: RawMethodHandler) -> Result<()> {
        if self
            .emitted_events
            .read()
            .expect("event map poisoned")
            .contains_key(&field.id)
            || self
                .methods
                .read()
                .expect("method map poisoned")
                .contains_key(&field.id)
        {
            return Err(GlycoError::DuplicateField(field.id));
        }
        self.methods
            .write()
            .expect("method map poisoned")
            .insert(field.id.clone(), RegisteredMethod { field, handler });
        self.mark_beacon_changed();
        Ok(())
    }

    fn insert_event(&self, field: EventField, argument_type: Option<TypeId>) -> Result<()> {
        if self
            .methods
            .read()
            .expect("method map poisoned")
            .contains_key(&field.id)
            || self
                .emitted_events
                .read()
                .expect("event map poisoned")
                .contains_key(&field.id)
        {
            return Err(GlycoError::DuplicateField(field.id));
        }
        self.emitted_events
            .write()
            .expect("event map poisoned")
            .insert(
                field.id.clone(),
                RegisteredEvent {
                    field,
                    argument_type,
                },
            );
        self.mark_beacon_changed();
        Ok(())
    }

    fn mark_beacon_changed(&self) {
        self.beacon_revision.send_modify(|revision| {
            *revision = revision.wrapping_add(1);
        });
    }

    fn build_beacon(&self) -> Beacon {
        let methods = self.methods.read().expect("method map poisoned");
        let events = self.emitted_events.read().expect("event map poisoned");
        let mut fields: Vec<Field> = methods
            .values()
            .map(|method| Field::Method(method.field.clone()))
            .chain(
                events
                    .values()
                    .map(|event| Field::Event(event.field.clone())),
            )
            .collect();
        fields.sort_by(|left, right| {
            left.id().cmp(right.id()).then_with(|| match (left, right) {
                (Field::Method(_), Field::Event(_)) => std::cmp::Ordering::Less,
                (Field::Event(_), Field::Method(_)) => std::cmp::Ordering::Greater,
                _ => std::cmp::Ordering::Equal,
            })
        });
        let mut beacon = Beacon::new(&self.id, fields);
        beacon.vendor = self.vendor.read().expect("vendor lock poisoned").clone();
        beacon
    }

    async fn send_full_beacon(&self) -> Result<()> {
        self.connexon
            .send(&Glycosyl::Beacon(self.build_beacon()))
            .await
    }

    fn find_remote_method(&self, gid: &str, fid: &str) -> Result<MethodField> {
        let presenters = self.presenters.read().expect("presenter map poisoned");
        let beacon = presenters
            .get(gid)
            .ok_or_else(|| GlycoError::FieldNotFound {
                node: gid.to_owned(),
                field: fid.to_owned(),
            })?;
        let field = beacon
            .beacon
            .fields
            .iter()
            .find(|field| field.id() == fid)
            .ok_or_else(|| GlycoError::FieldNotFound {
                node: gid.to_owned(),
                field: fid.to_owned(),
            })?;
        match field {
            Field::Method(method) => Ok(method.clone()),
            Field::Event(_) => Err(GlycoError::NotAMethod {
                node: gid.to_owned(),
                field: fid.to_owned(),
            }),
        }
    }

    fn process_beacon(&self, beacon: Beacon) {
        let signature = beacon_signature(&beacon);
        let now = Instant::now();
        let mut presenters = self.presenters.write().expect("presenter map poisoned");
        let event = match presenters.insert(
            beacon.id.clone(),
            PresenterRecord {
                beacon: beacon.clone(),
                signature: signature.clone(),
                last_seen: now,
            },
        ) {
            None => {
                if beacon.id == self.id && !self.loopback_presenter.load(Ordering::Acquire) {
                    None
                } else {
                    Some(PresenterEvent::Discovered(beacon))
                }
            }
            Some(previous) if previous.signature != signature => Some(PresenterEvent::Changed {
                previous: previous.beacon,
                current: beacon,
            }),
            Some(_) => None,
        };
        drop(presenters);
        if let Some(event) = event {
            let _ = self.presenter_events.send(event);
        }
    }

    fn process_heartbeat(&self, heartbeat: Heartbeat) {
        if let Some(presenter) = self
            .presenters
            .write()
            .expect("presenter map poisoned")
            .get_mut(&heartbeat.id)
        {
            presenter.last_seen = Instant::now();
        }
    }

    fn process_reply(&self, reply: Reply) {
        let Some(sender) = self
            .pending
            .lock()
            .expect("pending map poisoned")
            .remove(&reply.qid)
        else {
            return;
        };
        let result = match reply.error {
            Some(message) => Err(RemoteError { message }),
            None => Ok(reply.payload),
        };
        let _ = sender.send(result);
    }
}

impl Drop for NodeInner {
    fn drop(&mut self) {
        if let Some(cancellation) = self
            .active_cancellation
            .lock()
            .expect("active cancellation lock poisoned")
            .take()
        {
            cancellation.cancel();
        }
    }
}

async fn dispatch_loop(
    inner: Arc<NodeInner>,
    mut receiver: broadcast::Receiver<ConnexonEvent>,
    cancellation: CancellationToken,
) {
    loop {
        tokio::select! {
            () = cancellation.cancelled() => break,
            event = receiver.recv() => match event {
                Ok(ConnexonEvent::Message(message)) => dispatch_message(&inner, message, &cancellation),
                Ok(ConnexonEvent::Fault { peer, message }) => {
                    tracing::warn!(peer = peer.as_deref(), "connexon fault: {message}");
                }
                Ok(ConnexonEvent::PeerConnected(peer)) => {
                    tracing::debug!(%peer, "peer connected");
                }
                Ok(ConnexonEvent::PeerDisconnected(peer)) => {
                    tracing::debug!(%peer, "peer disconnected");
                }
                Err(broadcast::error::RecvError::Lagged(count)) => {
                    tracing::warn!(count, "connexon receiver lagged");
                }
                Err(broadcast::error::RecvError::Closed) => break,
            }
        }
    }
}

fn dispatch_message(inner: &Arc<NodeInner>, message: Glycosyl, cancellation: &CancellationToken) {
    match message {
        Glycosyl::Beacon(beacon) => inner.process_beacon(beacon),
        Glycosyl::Heartbeat(heartbeat) => inner.process_heartbeat(heartbeat),
        Glycosyl::Reply(reply) => inner.process_reply(reply),
        Glycosyl::Query(query) if query.gid == inner.id => {
            let inner = inner.clone();
            let cancellation = cancellation.child_token();
            tokio::spawn(async move { handle_query(inner, query, cancellation).await });
        }
        Glycosyl::Event(event) => {
            let handler = inner
                .event_handlers
                .read()
                .expect("event handler map poisoned")
                .get(&(event.gid.clone(), event.fid.clone()))
                .cloned();
            if let Some(handler) = handler {
                tokio::spawn(async move {
                    if let Err(error) = handler(event.arg).await {
                        tracing::warn!(gid = %event.gid, fid = %event.fid, "event handler failed: {error}");
                    }
                });
            }
        }
        _ => {}
    }
}

async fn handle_query(inner: Arc<NodeInner>, query: Query, cancellation: CancellationToken) {
    let permit = match inner.handler_semaphore.clone().try_acquire_owned() {
        Ok(permit) => permit,
        Err(_) => {
            send_error_reply(&inner, &query, "server is busy").await;
            return;
        }
    };

    let handler = inner
        .methods
        .read()
        .expect("method map poisoned")
        .get(&query.fid)
        .map(|method| method.handler.clone());
    let Some(handler) = handler else {
        drop(permit);
        send_error_reply(
            &inner,
            &query,
            &format!("field '{}' is not registered", query.fid),
        )
        .await;
        return;
    };

    let context = RequestContext {
        qid: query.qid,
        source_gid: query.source_gid.clone(),
        cancellation,
    };
    let result = handler(query.payload, context).await;
    drop(permit);

    let reply = match result {
        Ok(payload) => Reply {
            payload,
            qid: query.qid,
            target_gid: query.source_gid,
            error: None,
        },
        Err(error) => Reply {
            payload: None,
            qid: query.qid,
            target_gid: query.source_gid,
            error: Some(error.to_string()),
        },
    };
    if let Err(error) = inner.connexon.send(&Glycosyl::Reply(reply)).await {
        tracing::warn!(qid = %query.qid, "reply send failed: {error}");
    }
}

async fn send_error_reply(inner: &NodeInner, query: &Query, message: &str) {
    let reply = Reply {
        payload: None,
        qid: query.qid,
        target_gid: query.source_gid.clone(),
        error: Some(message.to_owned()),
    };
    if let Err(error) = inner.connexon.send(&Glycosyl::Reply(reply)).await {
        tracing::warn!(qid = %query.qid, "error reply send failed: {error}");
    }
}

async fn beacon_loop(
    inner: Arc<NodeInner>,
    mut changes: watch::Receiver<u64>,
    cancellation: CancellationToken,
) {
    let mut timer = tokio::time::interval(inner.heartbeat_interval);
    timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    timer.tick().await;
    let mut ticks_since_full = 0_u32;

    loop {
        let message = tokio::select! {
            () = cancellation.cancelled() => break,
            changed = changes.changed() => {
                if changed.is_err() {
                    break;
                }
                ticks_since_full = 0;
                Glycosyl::Beacon(inner.build_beacon())
            }
            _ = timer.tick() => {
                ticks_since_full += 1;
                if ticks_since_full >= inner.full_beacon_ticks {
                    ticks_since_full = 0;
                    Glycosyl::Beacon(inner.build_beacon())
                } else {
                    Glycosyl::Heartbeat(Heartbeat { id: inner.id.clone() })
                }
            }
        };
        if let Err(error) = inner.connexon.send(&message).await {
            tracing::warn!("beacon/heartbeat send failed: {error}");
        }
    }
}

async fn cleanup_loop(inner: Arc<NodeInner>, cancellation: CancellationToken) {
    let mut timer = tokio::time::interval(inner.cleanup_interval);
    timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    timer.tick().await;
    loop {
        tokio::select! {
            () = cancellation.cancelled() => break,
            _ = timer.tick() => {
                let now = Instant::now();
                let expired: Vec<Beacon> = {
                    let mut presenters = inner.presenters.write().expect("presenter map poisoned");
                    let expired_ids: Vec<String> = presenters
                        .iter()
                        .filter(|(_, record)| now.duration_since(record.last_seen) >= inner.presenter_expiry)
                        .map(|(id, _)| id.clone())
                        .collect();
                    expired_ids
                        .into_iter()
                        .filter_map(|id| presenters.remove(&id).map(|record| record.beacon))
                        .collect()
                };
                for beacon in expired {
                    let _ = inner.presenter_events.send(PresenterEvent::Expired(beacon));
                }
            }
        }
    }
}

fn beacon_signature(beacon: &Beacon) -> String {
    let mut value = serde_json::to_value(beacon).expect("beacon serialization cannot fail");
    if let Value::Object(object) = &mut value {
        object.remove("Timestamp");
    }
    serde_json::to_string(&value).expect("beacon serialization cannot fail")
}
