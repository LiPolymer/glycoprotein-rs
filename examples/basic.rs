use std::time::Duration;

use glycoprotein::{GlycoComplex, MethodField};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "PascalCase")]
struct AddRequest {
    left: i32,
    right: i32,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "PascalCase")]
struct AddResponse {
    sum: i32,
}

#[tokio::main]
async fn main() -> glycoprotein::Result<()> {
    let node = GlycoComplex::new("rust-basic")?;
    node.register_function(
        MethodField::new("add").friendly_name("Add two numbers"),
        |request: AddRequest, _context| async move {
            Ok(AddResponse {
                sum: request.left + request.right,
            })
        },
    )?;

    node.start().await?;

    // Broadcast delivery is asynchronous, including for this node's own beacon.
    tokio::time::timeout(Duration::from_secs(2), async {
        while !node
            .presenters()
            .iter()
            .any(|presenter| presenter.id == node.id())
        {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("local beacon was not observed");

    let response: AddResponse = node
        .call(
            "rust-basic",
            "add",
            &AddRequest {
                left: 20,
                right: 22,
            },
        )
        .await?
        .expect("add returned no payload");

    println!("20 + 22 = {}", response.sum);
    node.stop().await
}
