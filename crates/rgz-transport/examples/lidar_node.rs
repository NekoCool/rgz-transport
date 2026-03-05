///
/// This is a Rust rewrite of lidar_node.c from the following tutorial.
/// https://gazebosim.org/docs/harmonic/sensors
///
/// Run:
/// 1. Start Gazebo simulation (Terminal A):
///    `gz sim sensor_tutorial.sdf`
/// 2. Run this example (Terminal B):
///    `cargo run -p rgz-transport --example lidar_node`
///
/// Notes:
/// - The simulation must be in Play state to publish `/lidar`.
/// - Press Ctrl-C in Terminal B to exit this example.
///
use anyhow::Result;
use rgz_msgs::{LaserScan, Twist, Vector3d};
use rgz_transport::Node;
use tokio::signal;

#[tokio::main]
async fn main() -> Result<()> {
    let topic_pub = "/cmd_vel";
    let mut node = Node::new(None);
    let publisher = node.advertise::<Twist>(topic_pub, None)?;

    let topic_sub = "/lidar";
    node.subscribe(topic_sub, move |msg: LaserScan| {
        let all_more = msg.ranges.iter().all(|&range| range >= 1.0);
        let mut twist = Twist {
            ..Default::default()
        };
        if all_more {
            twist.linear = Some(Vector3d {
                x: 0.5,
                ..Default::default()
            });
            twist.angular = Some(Vector3d {
                z: 0.0,
                ..Default::default()
            });
        } else {
            twist.linear = Some(Vector3d {
                x: 0.0,
                ..Default::default()
            });
            twist.angular = Some(Vector3d {
                z: 0.5,
                ..Default::default()
            });
        };
        publisher.publish(twist).unwrap();
    })?;

    println!("Press Ctrl-C to exit.");
    signal::ctrl_c().await?;
    Ok(())
}
