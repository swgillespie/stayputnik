use std::sync::Arc;

use prost::Message;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, oneshot};

use crate::codec::{self, Decode};
use crate::krpc::schema as proto;
use crate::stream::{Stream, StreamId, StreamRegistry};
use crate::{Error, Result};

/// A connection to a kRPC server.
#[derive(Debug)]
pub struct Client {
    inner: ClientRef,
}

impl Client {
    /// Connects to a kRPC server and starts the connection tasks.
    ///
    /// Both the RPC connection and the stream connection are established
    /// here; the stream server is expected to listen on `rpc_port + 1`,
    /// the kRPC default. Connecting both up front means stream- and
    /// event-creating procedures work unconditionally — the server
    /// requires the stream connection to exist before they are called.
    pub async fn connect(name: &str, address: &str, rpc_port: u16) -> Result<Client> {
        let rpc_request = proto::ConnectionRequest {
            r#type: proto::connection_request::Type::Rpc as i32,
            client_name: name.to_string(),
            client_identifier: vec![],
        };
        let (rpc_stream, response) = open_connection(address, rpc_port, &rpc_request).await?;
        let client_identifier = response.client_identifier;

        let stream_request = proto::ConnectionRequest {
            r#type: proto::connection_request::Type::Stream as i32,
            client_name: String::new(),
            client_identifier: client_identifier.clone(),
        };
        let (stream_stream, _) = open_connection(address, rpc_port + 1, &stream_request).await?;

        let (rpc_tx, rpc_rx) = mpsc::unbounded_channel();
        tokio::spawn(rpc_actor(rpc_stream, rpc_rx));

        let registry = Arc::new(StreamRegistry::default());
        tokio::spawn(stream_reader(stream_stream, registry.clone()));

        Ok(Client {
            inner: ClientRef(Arc::new(Inner {
                rpc_tx,
                address: address.to_string(),
                client_identifier,
                registry,
            })),
        })
    }

    pub fn client_identifier(&self) -> &[u8] {
        &self.inner.0.client_identifier
    }

    pub fn into_shared(self) -> ClientRef {
        self.inner
    }
}

/// Connects to a kRPC listener and performs the connection handshake.
async fn open_connection(
    address: &str,
    port: u16,
    request: &proto::ConnectionRequest,
) -> Result<(TcpStream, proto::ConnectionResponse)> {
    let mut stream = TcpStream::connect((address, port))
        .await
        .map_err(|source| Error::Connect {
            address: address.to_string(),
            port,
            source,
        })?;

    send_message(&mut stream, request).await?;
    let response_bytes = receive_message(&mut stream).await?;
    let response = proto::ConnectionResponse::decode(response_bytes.as_slice())?;

    if response.status() != proto::connection_response::Status::Ok {
        return Err(Error::ConnectionRejected {
            status: response.status(),
            message: response.message,
        });
    }

    Ok((stream, response))
}

/// A cheaply clonable handle to the connection tasks.
#[derive(Clone)]
pub struct ClientRef(Arc<Inner>);

struct Inner {
    rpc_tx: mpsc::UnboundedSender<RpcRequest>,
    address: String,
    client_identifier: Vec<u8>,
    registry: Arc<StreamRegistry>,
}

impl std::fmt::Debug for ClientRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClientRef")
            .field("address", &self.0.address)
            .finish_non_exhaustive()
    }
}

struct RpcRequest {
    call: proto::ProcedureCall,
    resp: oneshot::Sender<Result<Vec<u8>>>,
}

impl ClientRef {
    pub(crate) async fn invoke(&self, call: proto::ProcedureCall) -> Result<Vec<u8>> {
        let (tx, rx) = oneshot::channel();
        self.0
            .rpc_tx
            .send(RpcRequest { call, resp: tx })
            .map_err(|_| Error::Disconnected)?;
        rx.await.map_err(|_| Error::Disconnected)?
    }

    /// Enqueues a call whose result nobody waits for. Used from `Drop`
    /// implementations, which cannot await.
    pub(crate) fn invoke_forget(&self, call: proto::ProcedureCall) {
        let (tx, _) = oneshot::channel();
        let _ = self.0.rpc_tx.send(RpcRequest { call, resp: tx });
    }

    /// Creates a server-side stream for the given procedure and returns a
    /// typed handle to it.
    pub(crate) async fn stream<T: Decode>(&self, call: proto::ProcedureCall) -> Result<Stream<T>> {
        let data = self
            .invoke(codec::call(
                "KRPC",
                "AddStream",
                vec![codec::arg(0, &call), codec::arg(1, &true)],
            ))
            .await?;
        let id = StreamId::new(proto::Stream::decode(data.as_slice())?.id);
        Ok(self.adopt_stream(id))
    }

    /// Subscribes to a stream the server already created (e.g. an event's
    /// underlying stream).
    pub(crate) fn adopt_stream<T: Decode>(&self, id: StreamId) -> Stream<T> {
        let registry = self.0.registry.clone();
        let rx = registry.register(id);
        Stream::new(id, self.clone(), registry, rx)
    }
}

/// Owns the RPC connection. Processes calls sequentially; exits when all
/// handles are dropped or the connection fails.
async fn rpc_actor(mut stream: TcpStream, mut rx: mpsc::UnboundedReceiver<RpcRequest>) {
    while let Some(request) = rx.recv().await {
        let result = perform_call(&mut stream, request.call).await;
        // Io/Decode failures leave the connection in an unknown state;
        // shut down so pending and future calls fail with Disconnected.
        let fatal = matches!(&result, Err(Error::Io(_)) | Err(Error::Decode(_)));
        let _ = request.resp.send(result);
        if fatal {
            break;
        }
    }
}

async fn perform_call(stream: &mut TcpStream, call: proto::ProcedureCall) -> Result<Vec<u8>> {
    let request = proto::Request { calls: vec![call] };
    send_message(stream, &request).await?;

    let response_bytes = receive_message(stream).await?;
    let response = proto::Response::decode(response_bytes.as_slice())?;

    if let Some(error) = response.error {
        return Err(Error::Rpc {
            service: error.service,
            name: error.name,
            description: error.description,
        });
    }

    let result = response
        .results
        .into_iter()
        .next()
        .ok_or(Error::EmptyResponse)?;

    if let Some(error) = result.error {
        return Err(Error::Procedure {
            service: error.service,
            name: error.name,
            description: error.description,
        });
    }

    Ok(result.value)
}

/// Reads `StreamUpdate` messages from the stream connection and routes them
/// to subscribed `Stream` handles until the connection fails.
async fn stream_reader(mut stream: TcpStream, registry: Arc<StreamRegistry>) {
    loop {
        let Ok(bytes) = receive_message(&mut stream).await else {
            break;
        };
        let Ok(update) = proto::StreamUpdate::decode(bytes.as_slice()) else {
            break;
        };
        registry.dispatch(update);
    }
    registry.close();
}

/// Builds a `ClientRef` that is not connected to anything. For tests that
/// need a client value but make no RPC calls.
#[cfg(test)]
pub(crate) fn test_client() -> ClientRef {
    let (rpc_tx, _) = mpsc::unbounded_channel();
    ClientRef(Arc::new(Inner {
        rpc_tx,
        address: String::new(),
        client_identifier: vec![],
        registry: Arc::new(StreamRegistry::default()),
    }))
}

async fn send_message(stream: &mut TcpStream, msg: &impl Message) -> Result<()> {
    let msg_bytes = msg.encode_to_vec();
    let mut buf = Vec::new();
    prost::encoding::encode_varint(msg_bytes.len() as u64, &mut buf);
    buf.extend_from_slice(&msg_bytes);
    stream.write_all(&buf).await?;
    Ok(())
}

async fn receive_message(stream: &mut TcpStream) -> Result<Vec<u8>> {
    let mut varint_buf = Vec::new();
    loop {
        let byte = stream.read_u8().await?;
        varint_buf.push(byte);
        if byte & 0x80 == 0 {
            break;
        }
    }
    let size = prost::encoding::decode_varint(&mut varint_buf.as_slice())? as usize;

    let mut msg_buf = vec![0u8; size];
    stream.read_exact(&mut msg_buf).await?;
    Ok(msg_buf)
}
