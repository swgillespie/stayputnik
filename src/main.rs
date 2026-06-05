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

    // Stream universal time at 10 Hz.
    let mut ut = sc.ut_stream().await?;
    ut.set_rate(10.0).await?;
    println!("Streaming universal time:");
    for _ in 0..5 {
        let t = ut.next().await?;
        println!("  UT = {t:.2} s");
    }

    // The same stream through StreamExt combinators.
    use tokio_stream::StreamExt;
    let ticks = (&mut ut)
        .take(3)
        .collect::<stayputnik::Result<Vec<f64>>>()
        .await?;
    println!("Collected via StreamExt: {ticks:?}");
    ut.remove().await?;

    Ok(())
}
