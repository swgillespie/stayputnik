#[doc(hidden)]
pub mod krpc {
    pub mod schema {
        include!(concat!(env!("OUT_DIR"), "/krpc.schema.rs"));
    }
}

mod error;
mod client;
pub mod codec;
pub mod services;

pub use error::{Error, Result};
pub use client::{Client, ClientRef};
