//! Streams telemetry from a kRPC server: the server pushes values at a
//! configured rate instead of being polled.
//!
//! Usage: cargo run --example streams [address]

use std::error::Error;

use stayputnik::services::space_center::SpaceCenter;
use tokio_stream::StreamExt;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let address = std::env::args().nth(1).unwrap_or_else(|| "127.0.0.1".to_string());

    let client = stayputnik::Client::connect("streams example", &address, 50000)
        .await?
        .into_shared();

    let sc = SpaceCenter::new(client);

    // Every value-returning method has a `*_stream()` variant.
    let mut ut = sc.ut_stream().await?;
    ut.set_rate(10.0).await?;

    // Consume updates one at a time...
    println!("Streaming universal time:");
    for _ in 0..5 {
        let t = ut.next().await?;
        println!("  UT = {t:.2} s");
    }

    // ...or through `StreamExt` combinators via the `futures_core::Stream` impl.
    let ticks = (&mut ut)
        .take(3)
        .collect::<stayputnik::Result<Vec<f64>>>()
        .await?;
    println!("Collected via StreamExt: {ticks:?}");

    // Streams are removed from the server when dropped; `remove()` does the
    // same but lets you observe errors.
    ut.remove().await?;

    Ok(())
}
