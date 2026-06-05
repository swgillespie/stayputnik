use crate::krpc::schema as proto;

/// Any error a kRPC client operation can produce.
///
/// Errors fall into three broad groups:
///
/// - **Connection setup**: [`Connect`](Error::Connect) and
///   [`ConnectionRejected`](Error::ConnectionRejected), only from
///   [`Client::connect`](crate::Client::connect).
/// - **Server-reported**: [`Rpc`](Error::Rpc) and
///   [`Procedure`](Error::Procedure) — the connection is healthy, but the
///   server declined or failed the call. These are usually recoverable
///   (e.g. "there is no active vessel right now").
/// - **Transport**: [`Disconnected`](Error::Disconnected),
///   [`EmptyResponse`](Error::EmptyResponse), [`Io`](Error::Io), and
///   [`Decode`](Error::Decode) — the connection failed or produced data
///   that could not be understood. The client cannot recover these;
///   reconnect with a fresh [`Client`](crate::Client).
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// A TCP connection to the server could not be established.
    ///
    /// Returned by [`Client::connect`](crate::Client::connect), which
    /// opens two connections: the RPC connection on the given port and
    /// the stream connection on the port after it. `port` identifies
    /// which of the two failed.
    #[error("failed to connect to {address}:{port}")]
    Connect {
        /// The address passed to `connect`.
        address: String,
        /// The port the failed connection was for: the RPC port, or
        /// `rpc_port + 1` for the stream connection.
        port: u16,
        /// The underlying socket error.
        source: std::io::Error,
    },

    /// The server actively refused the connection during the kRPC
    /// handshake (e.g. wrong connection type, or a malformed client name).
    #[error("connection rejected: {status:?}: {message}")]
    ConnectionRejected {
        /// The status code the server answered the handshake with.
        status: proto::connection_response::Status,
        /// The server's human-readable explanation.
        message: String,
    },

    /// The server rejected an entire request before executing it (for
    /// example, a call to a service or procedure that does not exist).
    ///
    /// Contrast with [`Procedure`](Error::Procedure), where the call was
    /// valid and ran but failed.
    #[error("RPC error in {service}.{name}: {description}")]
    Rpc {
        /// The service the failing call addressed.
        service: String,
        /// The server-side error type name.
        name: String,
        /// The server's human-readable explanation.
        description: String,
    },

    /// A procedure executed on the server and threw an error.
    ///
    /// This is the common failure mode for game-state problems: no active
    /// vessel, an argument out of range, an expression that failed to
    /// compile. The same error is also delivered through streams when the
    /// streamed procedure starts failing.
    #[error("procedure error in {service}.{name}: {description}")]
    Procedure {
        /// The service that defines the error type, e.g. `"KRPC"` or
        /// `"SpaceCenter"`. May be empty for generic server errors.
        service: String,
        /// The server-side exception type name, e.g.
        /// `"InvalidOperationException"` or `"ArgumentException"`.
        name: String,
        /// The server's human-readable explanation.
        description: String,
    },

    /// The server's response contained no result for the call. Indicates
    /// a server bug or protocol mismatch; should not occur in practice.
    #[error("empty response from server")]
    EmptyResponse,

    /// The connection task has shut down — the connection failed earlier,
    /// or the server closed it. Subsequent calls on any handle from the
    /// same [`Client`](crate::Client) return this; waiting streams and
    /// events also end with it.
    #[error("connection closed")]
    Disconnected,

    /// A socket read or write failed mid-operation. The connection is in
    /// an unknown state, so the connection task shuts down and later
    /// calls return [`Disconnected`](Error::Disconnected).
    #[error(transparent)]
    Io(#[from] std::io::Error),

    /// A server message or value could not be decoded. Also used for
    /// client-side construction mistakes detected while encoding (e.g. an
    /// expression lambda parameter used outside its lambda).
    #[error("protobuf decode error: {0}")]
    Decode(#[from] prost::DecodeError),
}

/// Shorthand for `Result` with this crate's [`Error`].
pub type Result<T> = std::result::Result<T, Error>;
