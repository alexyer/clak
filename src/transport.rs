use std::{fmt::Display, io, net::SocketAddr};

use serde::{Deserialize, Serialize};
use tokio::net::UdpSocket;

use crate::{message::Message, Address};

pub trait Transport: Sized {
    type TransportAddress: Address;
    type Error;

    fn bind(
        address: Self::TransportAddress,
    ) -> impl std::future::Future<Output = Result<Self, Self::Error>> + Send;

    /// Wait for the next received message.
    fn next(
        &self,
    ) -> impl std::future::Future<
        Output = Option<(Message<Self::TransportAddress>, Self::TransportAddress)>,
    > + Send;

    fn send(
        &self,
        msg: &Message<Self::TransportAddress>,
        target: &Self::TransportAddress,
    ) -> impl std::future::Future<Output = Result<usize, Self::Error>> + Send;

    fn address(&self) -> Self::TransportAddress;
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TcpAddress {
    host: String,
    port: u16,
}

impl From<SocketAddr> for TcpAddress {
    fn from(value: SocketAddr) -> Self {
        Self {
            host: value.ip().to_string(),
            port: value.port(),
        }
    }
}

impl Display for TcpAddress {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("{}:{}", self.host, self.port))
    }
}

impl Address for TcpAddress {}

impl TcpAddress {
    pub fn new(host: String, port: u16) -> Self {
        Self { host, port }
    }
}

pub struct UdpTransport {
    socket: UdpSocket,
}

impl Transport for UdpTransport {
    type TransportAddress = TcpAddress;
    type Error = io::Error;

    async fn bind(address: Self::TransportAddress) -> Result<Self, Self::Error> {
        let socket = UdpSocket::bind(address.to_string()).await?;

        log::info!(target: "udp-transport", "📞 Listening on {}", address);

        Ok(Self { socket })
    }

    async fn next(&self) -> Option<(Message<Self::TransportAddress>, Self::TransportAddress)> {
        // TODO(alexyer): unnecessary allocation
        let mut buf = [0; 1024];

        match self.socket.recv_from(&mut buf).await {
            Ok((size, peer)) => {
                if let Ok(msg) = bincode::deserialize(&buf[..size]) {
                    Some((msg, peer.into()))
                } else {
                    log::debug!(target: "udp-transport", "invalid message");
                    None
                }
            }
            Err(e) => {
                log::error!(target: "udp-transport", "❗️ failed to recv message: {}", e);
                None
            }
        }
    }

    async fn send(
        &self,
        msg: &Message<Self::TransportAddress>,
        target: &Self::TransportAddress,
    ) -> Result<usize, Self::Error> {
        let bytes = bincode::serialize(msg).expect("serialization should not fail");
        self.socket.send_to(&bytes, target.to_string()).await
    }

    fn address(&self) -> Self::TransportAddress {
        self.socket
            .local_addr()
            .expect("address must be present")
            .into()
    }
}
