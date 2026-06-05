//! Connects to a kRPC server and makes a few basic RPCs.
//!
//! Usage: cargo run --example hello [address]

use std::error::Error;

use stayputnik::services::space_center::SpaceCenter;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let address = std::env::args().nth(1).unwrap_or_else(|| "127.0.0.1".to_string());

    let client = stayputnik::Client::connect("hello example", &address, 50000)
        .await?
        .into_shared();

    let sc = SpaceCenter::new(client);

    match sc.active_vessel().await {
        Ok(vessel) => {
            let name = vessel.name().await?;
            println!("Active vessel: {name}");
        }
        Err(e) => {
            println!("Could not get active vessel (not in flight?): {e}");
        }
    }

    let bodies = sc.bodies().await?;
    println!("Celestial bodies:");
    for (name, body) in &bodies {
        let mass = body.mass().await?;
        println!("  {name}: mass = {mass:.3e} kg");
    }

    Ok(())
}
