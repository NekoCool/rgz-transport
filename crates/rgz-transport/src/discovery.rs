//! Legacy-compatible discovery datagram codec and route store.

use std::collections::HashMap;
use std::net::{Ipv4Addr, SocketAddrV4};
use std::sync::Arc;

use prost::Message;
use rgz_msgs::Discovery;
use rgz_msgs::discovery::{self, DiscContents, Type};
use tokio::net::UdpSocket;
use tokio::sync::{Mutex, mpsc};
use tokio::task::JoinHandle;

use crate::config::DiscoveryConfig;
use crate::error::TransportError;

pub(crate) const LEGACY_DISCOVERY_WIRE_VERSION: u32 = 10;
const LENGTH_PREFIX_BYTES: usize = 2;

#[derive(Default)]
pub(crate) struct DiscoveryStore {
    publishers: HashMap<(String, String, String), discovery::Publisher>,
}

#[derive(Debug, Clone)]
pub(crate) enum DiscoveryEvent {
    PublisherAdvertised(discovery::Publisher),
    PublisherWithdrawn(discovery::Publisher),
    DecodeError(String),
}

/// Owns the opt-in UDP multicast receive tasks for the two legacy channels.
pub(crate) struct DiscoveryRuntime {
    tasks: Vec<JoinHandle<()>>,
    sockets: Vec<Arc<UdpSocket>>,
    multicast_addr: SocketAddrV4,
    process_uuid: String,
}

impl DiscoveryRuntime {
    pub(crate) async fn start(
        config: &DiscoveryConfig,
        event_tx: mpsc::Sender<DiscoveryEvent>,
    ) -> Result<Self, TransportError> {
        if !config.enabled {
            return Ok(Self {
                tasks: Vec::new(),
                sockets: Vec::new(),
                multicast_addr: SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, 0),
                process_uuid: String::new(),
            });
        }

        let multicast_ip = config.multicast_ip.parse::<Ipv4Addr>().map_err(|error| {
            TransportError::Serialization(format!("invalid discovery multicast IP: {error}"))
        })?;
        let interface_ip = config
            .interface_ip
            .as_deref()
            .map(str::parse::<Ipv4Addr>)
            .transpose()
            .map_err(|error| {
                TransportError::Serialization(format!("invalid discovery interface IP: {error}"))
            })?
            .unwrap_or(Ipv4Addr::UNSPECIFIED);

        let store = Arc::new(Mutex::new(DiscoveryStore::default()));
        let mut tasks = Vec::with_capacity(2);
        let mut sockets = Vec::with_capacity(2);
        for port in [config.message_port, config.service_port] {
            let socket = Arc::new(
                UdpSocket::bind(SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, port))
                    .await
                    .map_err(|error| {
                        TransportError::TemporaryTransport(format!(
                            "bind discovery UDP port {port} failed: {error}"
                        ))
                    })?,
            );
            socket
                .join_multicast_v4(multicast_ip, interface_ip)
                .map_err(|error| {
                    TransportError::TemporaryTransport(format!(
                        "join discovery multicast group failed: {error}"
                    ))
                })?;

            sockets.push(Arc::clone(&socket));
            let store = Arc::clone(&store);
            let event_tx = event_tx.clone();
            tasks.push(tokio::spawn(async move {
                let mut buffer = vec![0_u8; u16::MAX as usize];
                loop {
                    let received = match socket.recv_from(&mut buffer).await {
                        Ok((received, _)) => received,
                        Err(error) => {
                            let _ = event_tx
                                .send(DiscoveryEvent::DecodeError(format!(
                                    "discovery receive failed: {error}"
                                )))
                                .await;
                            break;
                        }
                    };
                    match decode_datagram(&buffer[..received]) {
                        Ok(message) => {
                            let before = store.lock().await.apply(&message);
                            let kind = Type::try_from(message.r#type).ok();
                            for publisher in before {
                                let event = match kind {
                                    Some(Type::Advertise) => {
                                        DiscoveryEvent::PublisherAdvertised(publisher)
                                    }
                                    _ => DiscoveryEvent::PublisherWithdrawn(publisher),
                                };
                                if event_tx.send(event).await.is_err() {
                                    return;
                                }
                            }
                        }
                        Err(error) => {
                            if event_tx
                                .send(DiscoveryEvent::DecodeError(error.to_string()))
                                .await
                                .is_err()
                            {
                                return;
                            }
                        }
                    }
                }
            }));
        }
        Ok(Self {
            tasks,
            sockets,
            multicast_addr: SocketAddrV4::new(multicast_ip, config.message_port),
            process_uuid: format!("rgz-{}", std::process::id()),
        })
    }

    pub(crate) async fn subscribe(&self, topic: &str) -> Result<(), TransportError> {
        if self.sockets.is_empty() {
            return Ok(());
        }
        let message = Discovery {
            version: LEGACY_DISCOVERY_WIRE_VERSION,
            process_uuid: self.process_uuid.clone(),
            r#type: Type::Subscribe as i32,
            flags: None,
            disc_contents: Some(DiscContents::Sub(discovery::Subscriber {
                topic: topic.to_string(),
            })),
            header: None,
        };
        let datagram = encode_datagram(&message)?;
        for socket in &self.sockets {
            socket
                .send_to(&datagram, self.multicast_addr)
                .await
                .map_err(|error| {
                    TransportError::TemporaryTransport(format!(
                        "send discovery subscribe failed: {error}"
                    ))
                })?;
        }
        Ok(())
    }

    pub(crate) async fn stop(mut self) {
        for task in self.tasks.drain(..) {
            task.abort();
        }
    }
}

impl DiscoveryStore {
    pub(crate) fn apply(&mut self, message: &Discovery) -> Vec<discovery::Publisher> {
        let Ok(message_type) = Type::try_from(message.r#type) else {
            return Vec::new();
        };
        let Some(DiscContents::Pub(publisher)) = message.disc_contents.as_ref() else {
            return Vec::new();
        };

        let key = (
            publisher.topic.clone(),
            publisher.process_uuid.clone(),
            publisher.node_uuid.clone(),
        );
        match message_type {
            Type::Advertise => {
                self.publishers.insert(key, publisher.clone());
                vec![publisher.clone()]
            }
            Type::Unadvertise | Type::EndConnection => {
                self.publishers.remove(&key).into_iter().collect()
            }
            Type::Bye => {
                let process_uuid = message.process_uuid.as_str();
                let removed = self
                    .publishers
                    .extract_if(|(_, process, _), _| process == process_uuid)
                    .map(|(_, publisher)| publisher)
                    .collect();
                removed
            }
            _ => Vec::new(),
        }
    }

    pub(crate) fn publishers_for_topic(&self, topic: &str) -> Vec<&discovery::Publisher> {
        self.publishers
            .values()
            .filter(|publisher| publisher.topic == topic)
            .collect()
    }
}

pub(crate) fn encode_datagram(message: &Discovery) -> Result<Vec<u8>, TransportError> {
    let payload_len = message.encoded_len();
    let payload_len = u16::try_from(payload_len).map_err(|_| {
        TransportError::Serialization("discovery message exceeds u16 datagram size".to_string())
    })?;
    let mut datagram = Vec::with_capacity(LENGTH_PREFIX_BYTES + payload_len as usize);
    datagram.extend_from_slice(&payload_len.to_le_bytes());
    message.encode(&mut datagram).map_err(|error| {
        TransportError::Serialization(format!("encode discovery message failed: {error}"))
    })?;
    Ok(datagram)
}

pub(crate) fn decode_datagram(datagram: &[u8]) -> Result<Discovery, TransportError> {
    if datagram.len() < LENGTH_PREFIX_BYTES {
        return Err(TransportError::Serialization(
            "discovery datagram is missing length prefix".to_string(),
        ));
    }
    let payload_len = u16::from_le_bytes([datagram[0], datagram[1]]) as usize;
    if datagram.len() != LENGTH_PREFIX_BYTES + payload_len {
        return Err(TransportError::Serialization(
            "discovery datagram length prefix does not match payload".to_string(),
        ));
    }
    Discovery::decode(&datagram[LENGTH_PREFIX_BYTES..]).map_err(|error| {
        TransportError::Serialization(format!("decode discovery message failed: {error}"))
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decodes_legacy_subscribe_fixture() {
        let fixture = "1b00100a1a027031200232110a0f402f64656d6f402f63686174746572";
        let datagram = (0..fixture.len())
            .step_by(2)
            .map(|index| u8::from_str_radix(&fixture[index..index + 2], 16).expect("fixture hex"))
            .collect::<Vec<_>>();
        let message = decode_datagram(&datagram).expect("decode fixture");

        assert_eq!(message.version, LEGACY_DISCOVERY_WIRE_VERSION);
        assert_eq!(message.process_uuid, "p1");
        assert_eq!(Type::try_from(message.r#type), Ok(Type::Subscribe));
    }
}
