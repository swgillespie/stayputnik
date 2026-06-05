use crate::krpc::schema as proto;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("failed to connect to {address}:{port}")]
    Connect {
        address: String,
        port: u16,
        source: std::io::Error,
    },

    #[error("connection rejected: {status:?}: {message}")]
    ConnectionRejected {
        status: proto::connection_response::Status,
        message: String,
    },

    #[error("RPC error in {service}.{name}: {description}")]
    Rpc {
        service: String,
        name: String,
        description: String,
    },

    #[error("procedure error in {service}.{name}: {description}")]
    Procedure {
        service: String,
        name: String,
        description: String,
    },

    #[error("empty response from server")]
    EmptyResponse,

    #[error("connection closed")]
    Disconnected,

    #[error(transparent)]
    Io(#[from] std::io::Error),

    #[error("protobuf decode error: {0}")]
    Decode(#[from] prost::DecodeError),
}

pub type Result<T> = std::result::Result<T, Error>;
