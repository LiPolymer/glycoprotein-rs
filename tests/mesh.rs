use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use glycoprotein::{
    EventField, GlycoComplex, GlycoError, HandlerError, MethodField, PresenterEvent,
    UnixDomainMeshConnexon,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::json;
use tempfile::TempDir;
use tokio::sync::oneshot;

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "PascalCase")]
struct AddRequest {
    left: i32,
    right: i32,
}

#[derive(Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "PascalCase")]
struct AddResponse {
    sum: i32,
}

#[derive(Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "PascalCase")]
struct Progress {
    percent: u8,
}

fn node(id: &str, directory: &TempDir) -> GlycoComplex {
    GlycoComplex::builder(id)
        .connexon(UnixDomainMeshConnexon::with_socket_directory(id, directory.path()).unwrap())
        .build()
        .unwrap()
}

async fn wait_until(mut condition: impl FnMut() -> bool) {
    tokio::time::timeout(Duration::from_secs(8), async {
        while !condition() {
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("condition was not met before the timeout");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_nodes_discover_and_exchange_rpc_actions_and_events() {
    let directory = tempfile::tempdir().unwrap();
    let server = node("rust-server", &directory);
    let client = node("rust-client", &directory);

    server.set_vendor(Some("Rust integration test".into()));
    server
        .register_function(
            MethodField::new("add").description("Adds two signed integers"),
            |request: AddRequest, _| async move {
                Ok(AddResponse {
                    sum: request.left + request.right,
                })
            },
        )
        .unwrap();
    server
        .register_function(MethodField::new("fail"), |_: AddRequest, _| async move {
            Err::<AddResponse, _>(HandlerError::new("deliberate failure"))
        })
        .unwrap();

    let action_count = Arc::new(AtomicUsize::new(0));
    let action_count_for_handler = action_count.clone();
    server
        .register_action(MethodField::new("touch"), move |_| {
            let action_count = action_count_for_handler.clone();
            async move {
                action_count.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }
        })
        .unwrap();
    server
        .register_event::<Progress>(EventField::new("progress"))
        .unwrap();

    let (progress_sender, progress_receiver) = oneshot::channel();
    let progress_sender = Arc::new(Mutex::new(Some(progress_sender)));
    client.on_event("rust-server", "progress", move |progress: Progress| {
        let progress_sender = progress_sender.clone();
        async move {
            if let Some(sender) = progress_sender.lock().unwrap().take() {
                let _ = sender.send(progress);
            }
            Ok(())
        }
    });

    let mut presenter_events = client.subscribe_presenters();
    server.start().await.unwrap();
    client.start().await.unwrap();

    wait_until(|| {
        client.presenters().iter().any(|beacon| {
            beacon.id == "rust-server"
                && beacon.vendor.as_deref() == Some("Rust integration test")
                && beacon.fields.iter().any(|field| field.id() == "add")
                && beacon.fields.iter().any(|field| field.id() == "progress")
        })
    })
    .await;

    let response: AddResponse = client
        .call(
            "rust-server",
            "add",
            &AddRequest {
                left: 19,
                right: 23,
            },
        )
        .await
        .unwrap()
        .expect("add must return a payload");
    assert_eq!(response, AddResponse { sum: 42 });

    let invalid = client
        .call_raw(
            "rust-server",
            "add",
            Some(json!({"Left": "not an integer", "Right": 1})),
        )
        .await
        .unwrap_err();
    assert!(matches!(invalid, GlycoError::Validation(_)));

    let remote = client
        .call::<_, AddResponse>("rust-server", "fail", &AddRequest { left: 1, right: 2 })
        .await
        .unwrap_err();
    assert!(
        matches!(remote, GlycoError::Remote(ref error) if error.message == "deliberate failure")
    );

    client.do_action("rust-server", "touch").await.unwrap();
    wait_until(|| action_count.load(Ordering::SeqCst) == 1).await;

    server
        .emit("progress", &Progress { percent: 75 })
        .await
        .unwrap();
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(2), progress_receiver)
            .await
            .unwrap()
            .unwrap(),
        Progress { percent: 75 }
    );

    assert!(server.remove_field("add"));
    wait_until(|| {
        client
            .presenters()
            .iter()
            .find(|beacon| beacon.id == "rust-server")
            .is_some_and(|beacon| !beacon.fields.iter().any(|field| field.id() == "add"))
    })
    .await;
    let mut saw_changed = false;
    while let Ok(event) = presenter_events.try_recv() {
        if matches!(event, PresenterEvent::Changed { ref current, .. } if current.id == "rust-server")
        {
            saw_changed = true;
        }
    }
    assert!(saw_changed, "field removal must publish a Changed event");

    server.stop().await.unwrap();
    wait_until(|| {
        !client
            .presenters()
            .iter()
            .any(|beacon| beacon.id == "rust-server")
    })
    .await;
    client.stop().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn transport_can_restart_and_reclaim_its_socket() {
    let directory = tempfile::tempdir().unwrap();
    let node = node("restartable", &directory);

    node.start().await.unwrap();
    assert!(directory.path().join("restartable.sock").exists());
    node.stop().await.unwrap();
    assert!(!directory.path().join("restartable.sock").exists());

    node.start().await.unwrap();
    assert!(directory.path().join("restartable.sock").exists());
    node.stop().await.unwrap();
}
