use anyhow::Result;
use std::env;
use tokio::signal;

use rgz_msgs::StringMsg;
use rgz_transport::Node;

#[tokio::main]
async fn main() -> Result<()> {
    // Rust 2024: mutating process-wide environment is unsafe.
    unsafe {
        env::set_var("GZ_IP", "172.17.0.1");
    }

    let topic = "/foo";
    let mut node = Node::new(None);

    node.subscribe(topic, move |msg: StringMsg| {
        println!("RECV: {}", msg.data);
    })?;

    println!("Press Ctrl-C to exit.");
    signal::ctrl_c().await?;
    Ok(())
}
