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
pub use stream::Stream;
