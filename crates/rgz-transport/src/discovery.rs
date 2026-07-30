//! Legacy-compatible discovery datagram codec and route store.

use std::collections::HashMap;

use prost::Message;
use rgz_msgs::Discovery;
use rgz_msgs::discovery::{self, DiscContents, Type};

use crate::error::TransportError;

pub(crate) const LEGACY_DISCOVERY_WIRE_VERSION: u32 = 10;
const LENGTH_PREFIX_BYTES: usize = 2;

#[derive(Default)]
pub(crate) struct DiscoveryStore {
    publishers: HashMap<(String, String, String), discovery::Publisher>,
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
