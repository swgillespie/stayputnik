//! An async client for [kRPC](https://krpc.github.io/krpc/), the Remote
//! Procedure Call server mod for Kerbal Space Program — control your
//! rockets from Rust.
//!
//! Named after the Stayputnik Mk. 1, KSP's first probe core: the part that
//! lets you fly a craft with no kerbal aboard. This crate is that part, in
//! library form.
//!
//! # Connecting
//!
//! [`Client::connect`] establishes the connection and spawns a background
//! task that owns it; [`Client::into_shared`] yields a cheaply clonable
//! [`ClientRef`] that the service APIs take:
//!
//! ```no_run
//! use stayputnik::services::space_center::SpaceCenter;
//!
//! #[tokio::main]
//! async fn main() -> stayputnik::Result<()> {
//!     let client = stayputnik::Client::connect("my program", "127.0.0.1", 50000)
//!         .await?
//!         .into_shared();
//!
//!     let sc = SpaceCenter::new(client);
//!     let vessel = sc.active_vessel().await?;
//!     println!("Flying {}", vessel.name().await?);
//!     Ok(())
//! }
//! ```
//!
//! # Services
//!
//! Everything the server exposes lives under [`services`]: one module per
//! kRPC service ([`services::space_center`] is the main game API), with
//! remote classes as handle structs and properties/methods as async
//! functions. See the [`services`] module docs for how the mapping works.
//!
//! # Streams
//!
//! Polling an RPC in a loop is too slow for a control loop. Every
//! value-returning method has a `*_stream()` variant that asks the server
//! to push updates instead, returning a [`Stream<T>`](Stream):
//!
//! ```no_run
//! # async fn demo(sc: stayputnik::services::space_center::SpaceCenter) -> stayputnik::Result<()> {
//! let mut altitude = sc.active_vessel().await?
//!     .flight(None).await?
//!     .mean_altitude_stream().await?;
//! loop {
//!     let m = altitude.next().await?;
//!     println!("{m:.0} m");
//! }
//! # }
//! ```
//!
//! Streams track the latest value ([`Stream::get`]/[`Stream::next`]), work
//! with `StreamExt` combinators via [`futures_core::Stream`], and remove
//! themselves from the server when dropped.

#[doc(hidden)]
pub mod krpc {
    pub mod schema {
        include!(concat!(env!("OUT_DIR"), "/krpc.schema.rs"));
    }
}

mod error;
mod client;
mod stream;
pub mod codec;
pub mod services;

pub use error::{Error, Result};
pub use client::{Client, ClientRef};
pub use stream::{Stream, StreamId};
