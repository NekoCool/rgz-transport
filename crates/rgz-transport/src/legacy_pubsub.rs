//! Legacy-compatible PUB/SUB multipart codec.
//!
//! The codec is deliberately crate-private: callers interact with the v2
//! transport API and its generic headers, never with legacy frame positions.

use zeromq::ZmqMessage;

use crate::actor::{MessageHeaders, MessagePayload};
use crate::error::TransportError;

const PUBLISHER_ADDRESS_HEADER: &str = "rgz.publisher.address";
const MESSAGE_TYPE_HEADER: &str = "rgz.message.type";

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct LegacyPublication {
    pub(crate) topic: String,
    pub(crate) publisher_address: String,
    pub(crate) payload: MessagePayload,
    pub(crate) message_type: String,
}

/// Encodes the four-frame legacy publication layout when the generic message
/// metadata supplies both fields required by that protocol.
pub(crate) fn encode_if_configured(
    topic: &str,
    payload: &[u8],
    headers: Option<&MessageHeaders>,
) -> Result<Option<ZmqMessage>, TransportError> {
    let Some(headers) = headers else {
        return Ok(None);
    };
    let publisher_address = headers.get(PUBLISHER_ADDRESS_HEADER);
    let message_type = headers.get(MESSAGE_TYPE_HEADER);

    match (publisher_address, message_type) {
        (None, None) => Ok(None),
        (Some(publisher_address), Some(message_type)) => {
            if topic.is_empty() || publisher_address.is_empty() || message_type.is_empty() {
                return Err(TransportError::Serialization(
                    "legacy publish metadata must not be empty".to_string(),
                ));
            }

            let mut message: ZmqMessage = topic.to_string().into();
            message.push_back(publisher_address.to_string().into());
            message.push_back(payload.to_vec().into());
            message.push_back(message_type.to_string().into());
            Ok(Some(message))
        }
        _ => Err(TransportError::Serialization(
            "legacy publish metadata requires both publisher address and message type".to_string(),
        )),
    }
}

/// Decodes exactly the legacy four-frame publication layout.
pub(crate) fn decode(message: &ZmqMessage) -> Result<LegacyPublication, TransportError> {
    if message.len() != 4 {
        return Err(TransportError::Serialization(format!(
            "legacy publish requires 4 frames, received {}",
            message.len()
        )));
    }

    let text_frame = |index: usize, field: &str| {
        let frame = message.get(index).ok_or_else(|| {
            TransportError::Serialization(format!("legacy publish missing {field} frame"))
        })?;
        String::from_utf8(frame.to_vec()).map_err(|_| {
            TransportError::Serialization(format!("legacy publish {field} is not utf-8"))
        })
    };

    let topic = text_frame(0, "topic")?;
    let publisher_address = text_frame(1, "publisher address")?;
    let payload = message
        .get(2)
        .ok_or_else(|| {
            TransportError::Serialization("legacy publish missing payload frame".to_string())
        })?
        .to_vec();
    let message_type = text_frame(3, "message type")?;

    if topic.is_empty() || publisher_address.is_empty() || message_type.is_empty() {
        return Err(TransportError::Serialization(
            "legacy publish text frames must not be empty".to_string(),
        ));
    }

    Ok(LegacyPublication {
        topic,
        publisher_address,
        payload,
        message_type,
    })
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;

    #[test]
    fn encodes_legacy_fixture_frames() {
        let headers = BTreeMap::from([
            (
                PUBLISHER_ADDRESS_HEADER.to_string(),
                "tcp://192.0.2.10:34567".to_string(),
            ),
            (
                MESSAGE_TYPE_HEADER.to_string(),
                "gz.msgs.StringMsg".to_string(),
            ),
        ]);
        let message =
            encode_if_configured("@/demo@/chatter", &[0x0a, 0x02, b'o', b'k'], Some(&headers))
                .expect("encode")
                .expect("legacy message");

        assert_eq!(message.len(), 4);
        assert_eq!(message.get(0).expect("topic").to_vec(), b"@/demo@/chatter");
        assert_eq!(
            message.get(1).expect("address").to_vec(),
            b"tcp://192.0.2.10:34567"
        );
        assert_eq!(
            message.get(2).expect("payload").to_vec(),
            [0x0a, 0x02, b'o', b'k']
        );
        assert_eq!(message.get(3).expect("type").to_vec(), b"gz.msgs.StringMsg");
    }

    #[test]
    fn decodes_legacy_fixture_frames() {
        let mut message: ZmqMessage = "@/demo@/chatter".into();
        message.push_back("tcp://192.0.2.10:34567".into());
        message.push_back(vec![0x0a, 0x02, b'o', b'k'].into());
        message.push_back("gz.msgs.StringMsg".into());

        assert_eq!(
            decode(&message).expect("decode"),
            LegacyPublication {
                topic: "@/demo@/chatter".to_string(),
                publisher_address: "tcp://192.0.2.10:34567".to_string(),
                payload: vec![0x0a, 0x02, b'o', b'k'],
                message_type: "gz.msgs.StringMsg".to_string(),
            }
        );
    }
}
