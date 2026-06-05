//! Waits for a server-side event built from an expression tree.
//!
//! The server evaluates the expression every physics tick and pushes when
//! it becomes true — no polling traffic at all while waiting.
//!
//! Usage: cargo run --example events [address]

use std::error::Error;

use stayputnik::expr::Expr;
use stayputnik::services::krpc::KRPC;
use stayputnik::services::space_center::SpaceCenter;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let address = std::env::args().nth(1).unwrap_or_else(|| "127.0.0.1".to_string());

    let client = stayputnik::Client::connect("events example", &address, 50000)
        .await?
        .into_shared();

    let krpc = KRPC::new(client.clone());
    let sc = SpaceCenter::new(client.clone());

    // Build a server-side expression: `SpaceCenter.UT > now + 3`.
    let now = sc.ut().await?;
    let target = now + 3.0;
    let expr = Expr::from(sc.ut_call()).gt(target).build(&client).await?;

    println!("UT is {now:.2}; waiting server-side until it passes {target:.2}...");
    // An Event is awaitable directly; use `event.wait().await` instead to
    // wait for the same event more than once.
    let event = krpc.add_event(&expr).await?;
    event.await?;
    println!("Event fired! UT is now {:.2}", sc.ut().await?);

    Ok(())
}
