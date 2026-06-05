use stayputnik::services::space_center::SpaceCenter;
use std::error::Error;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let client = stayputnik::Client::connect("Rust Hello World", "192.168.16.1", 50000)
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
