use std::sync::Arc;

use prost::Message;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, oneshot, Mutex};

use crate::codec::{arg, Decode};
use crate::krpc::schema as proto;
use crate::stream::{Stream, StreamRegistry};
use crate::{Error, Result};

/// A connection to a kRPC server.
#[derive(Debug)]
pub struct Client {
    inner: ClientRef,
}

impl Client {
    /// Connects to a kRPC server and starts the connection task.
    ///
    /// The stream server is assumed to listen on `rpc_port + 1` (the kRPC
    /// default); it is only connected to when the first stream is created.
    pub async fn connect(name: &str, address: &str, rpc_port: u16) -> Result<Client> {
        let mut stream =
            TcpStream::connect((address, rpc_port))
                .await
                .map_err(|source| Error::Connect {
                    address: address.to_string(),
                    port: rpc_port,
                    source,
                })?;

        let request = proto::ConnectionRequest {
            r#type: proto::connection_request::Type::Rpc as i32,
            client_name: name.to_string(),
            client_identifier: vec![],
        };
        send_message(&mut stream, &request).await?;

        let response_bytes = receive_message(&mut stream).await?;
        let response = proto::ConnectionResponse::decode(response_bytes.as_slice())?;

        if response.status() != proto::connection_response::Status::Ok {
            return Err(Error::ConnectionRejected {
                status: response.status(),
                message: response.message,
            });
        }

        let (rpc_tx, rpc_rx) = mpsc::unbounded_channel();
        tokio::spawn(rpc_actor(stream, rpc_rx));

        Ok(Client {
            inner: ClientRef(Arc::new(Inner {
                rpc_tx,
                address: address.to_string(),
                stream_port: rpc_port + 1,
                client_identifier: response.client_identifier,
                streams: Mutex::new(None),
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

/// A cheaply clonable handle to the connection task.
#[derive(Clone)]
pub struct ClientRef(Arc<Inner>);

struct Inner {
    rpc_tx: mpsc::UnboundedSender<RpcRequest>,
    address: String,
    stream_port: u16,
    client_identifier: Vec<u8>,
    /// Stream connection state; established lazily on first stream creation.
    streams: Mutex<Option<Arc<StreamRegistry>>>,
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
    pub(crate) async fn invoke(
        &self,
        service: &str,
        procedure: &str,
        arguments: &[proto::Argument],
    ) -> Result<Vec<u8>> {
        let call = procedure_call(service, procedure, arguments.to_vec());
        let (tx, rx) = oneshot::channel();
        self.0
            .rpc_tx
            .send(RpcRequest { call, resp: tx })
            .map_err(|_| Error::Disconnected)?;
        rx.await.map_err(|_| Error::Disconnected)?
    }

    /// Enqueues a call whose result nobody waits for. Used from `Drop`
    /// implementations, which cannot await.
    pub(crate) fn invoke_forget(
        &self,
        service: &str,
        procedure: &str,
        arguments: Vec<proto::Argument>,
    ) {
        let call = procedure_call(service, procedure, arguments);
        let (tx, _) = oneshot::channel();
        let _ = self.0.rpc_tx.send(RpcRequest { call, resp: tx });
    }

    /// Creates a server-side stream for the given procedure and returns a
    /// typed handle to it.
    pub(crate) async fn stream<T: Decode>(
        &self,
        service: &str,
        procedure: &str,
        arguments: &[proto::Argument],
    ) -> Result<Stream<T>> {
        let registry = self.ensure_stream_connection().await?;
        let call = procedure_call(service, procedure, arguments.to_vec());
        let data = self
            .invoke("KRPC", "AddStream", &[arg(0, &call), arg(1, &true)])
            .await?;
        let id = proto::Stream::decode(data.as_slice())?.id;
        let rx = registry.register(id);
        Ok(Stream::new(id, self.clone(), registry, rx))
    }

    async fn ensure_stream_connection(&self) -> Result<Arc<StreamRegistry>> {
        let mut guard = self.0.streams.lock().await;
        if let Some(registry) = &*guard {
            return Ok(registry.clone());
        }

        let address = self.0.address.as_str();
        let mut stream = TcpStream::connect((address, self.0.stream_port))
            .await
            .map_err(|source| Error::Connect {
                address: address.to_string(),
                port: self.0.stream_port,
                source,
            })?;

        let request = proto::ConnectionRequest {
            r#type: proto::connection_request::Type::Stream as i32,
            client_name: String::new(),
            client_identifier: self.0.client_identifier.clone(),
        };
        send_message(&mut stream, &request).await?;
        let response_bytes = receive_message(&mut stream).await?;
        let response = proto::ConnectionResponse::decode(response_bytes.as_slice())?;
        if response.status() != proto::connection_response::Status::Ok {
            return Err(Error::ConnectionRejected {
                status: response.status(),
                message: response.message,
            });
        }

        let registry = Arc::new(StreamRegistry::default());
        tokio::spawn(stream_reader(stream, registry.clone()));
        *guard = Some(registry.clone());
        Ok(registry)
    }
}

fn procedure_call(
    service: &str,
    procedure: &str,
    arguments: Vec<proto::Argument>,
) -> proto::ProcedureCall {
    proto::ProcedureCall {
        service: service.to_string(),
        procedure: procedure.to_string(),
        arguments,
        ..Default::default()
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
        stream_port: 0,
        client_identifier: vec![],
        streams: Mutex::new(None),
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
