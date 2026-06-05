use std::sync::Arc;

use prost::Message;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::Mutex;

use crate::krpc::schema as proto;
use crate::{Error, Result};

#[derive(Debug)]
pub struct Client {
    stream: TcpStream,
    client_identifier: Vec<u8>,
}

impl Client {
    pub fn client_identifier(&self) -> &[u8] {
        &self.client_identifier
    }

    pub fn into_shared(self) -> ClientRef {
        ClientRef(Arc::new(Mutex::new(self)))
    }

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

        Ok(Client {
            stream,
            client_identifier: response.client_identifier,
        })
    }

    pub(crate) async fn invoke(
        &mut self,
        service: &str,
        procedure: &str,
        arguments: &[proto::Argument],
    ) -> Result<Vec<u8>> {
        let request = proto::Request {
            calls: vec![proto::ProcedureCall {
                service: service.to_string(),
                procedure: procedure.to_string(),
                arguments: arguments.to_vec(),
                ..Default::default()
            }],
        };
        send_message(&mut self.stream, &request).await?;

        let response_bytes = receive_message(&mut self.stream).await?;
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
}

#[derive(Clone, Debug)]
pub struct ClientRef(Arc<Mutex<Client>>);

impl ClientRef {
    pub(crate) async fn invoke(
        &self,
        service: &str,
        procedure: &str,
        arguments: &[proto::Argument],
    ) -> Result<Vec<u8>> {
        self.0
            .lock()
            .await
            .invoke(service, procedure, arguments)
            .await
    }
}

/// Builds a `ClientRef` backed by a loopback socket that is never used.
/// For tests that need a client value but make no RPC calls.
#[cfg(test)]
pub(crate) async fn test_client() -> ClientRef {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (stream, _accepted) = tokio::join!(TcpStream::connect(addr), listener.accept());
    Client {
        stream: stream.unwrap(),
        client_identifier: vec![],
    }
    .into_shared()
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
