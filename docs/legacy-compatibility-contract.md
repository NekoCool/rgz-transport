# Legacy wire and discovery compatibility contract

## Status and scope

This document fixes the compatibility boundary for transport v2. It is based
on the behavior in `crates/rgz-transport-legacy`; it is the contract for the
adapters introduced by issues #38 through #40.

The v2 Rust API is intentionally not source-compatible with the legacy `Node`
API. Compatibility is required only for network traffic and discovery:

- ZeroMQ PUB/SUB traffic;
- ZeroMQ service request/reply traffic; and
- UDP multicast discovery traffic and its configuration.

The adapter owns legacy frame layouts, socket identities, and discovery
metadata. They must not appear in the public Tokio-native API.

## Terms and invariants

All textual ZeroMQ frames use UTF-8. Payload frames are opaque bytes. A topic
used on the wire is the legacy fully-qualified topic, for example
`@/demo@/chatter`; it includes the partition and the resolved namespace. The
new API may normalize and remap a user topic, but it must perform that work
before consulting discovery or encoding a legacy frame.

Message type and service request/response type are the legacy protobuf type
name strings. A route is eligible only when its advertised type strings match
the type strings requested by the local operation.

An adapter must preserve every frame in the following tables byte-for-byte.
It may reject malformed, missing, overlong, or non-UTF-8 text frames with a
structured serialization error; it must not crash the actor.

## PUB/SUB protocol

A legacy message publisher binds a PUB endpoint. A subscriber connects a SUB
socket to that endpoint and installs the fully-qualified topic as the ZeroMQ
subscription prefix. Each publication contains exactly four multipart frames:

| Index | Field | Encoding |
| ---: | --- | --- |
| 0 | topic | UTF-8 fully-qualified topic |
| 1 | publisher address | UTF-8 ZeroMQ endpoint |
| 2 | payload | opaque bytes |
| 3 | message type | UTF-8 protobuf type name |

The PUB/SUB adapter must consume all four frames. In particular, treating the
second frame as the payload (the current v2 two-frame decoder behavior) is
not compatible.

## Service protocol

Legacy service routing uses ZeroMQ ROUTER sockets and endpoint/identity data
obtained from service discovery. ROUTER routing envelopes are handled by the
socket implementation; the application frames below are the frames sent by
the legacy transport and expected by its handlers.

### Request

The requester connects to the discovered service endpoint and sends these
nine application frames in order:

| Index | Field | Encoding |
| ---: | --- | --- |
| 0 | replier identity | UTF-8 socket ID from service discovery |
| 1 | topic | UTF-8 fully-qualified topic |
| 2 | requester endpoint | UTF-8 endpoint accepting replies |
| 3 | requester identity | UTF-8 requester socket ID |
| 4 | node UUID | UTF-8 requesting node UUID |
| 5 | request UUID | UTF-8 request correlation UUID |
| 6 | request payload | opaque bytes |
| 7 | request type | UTF-8 protobuf type name |
| 8 | response type | UTF-8 protobuf type name |

### Reply

The service connects to the requester endpoint and sends these six
application frames in order:

| Index | Field | Encoding |
| ---: | --- | --- |
| 0 | requester identity | UTF-8 requester socket ID |
| 1 | topic | UTF-8 fully-qualified topic |
| 2 | node UUID | UTF-8 requesting node UUID |
| 3 | request UUID | UTF-8 request correlation UUID |
| 4 | response payload | opaque bytes |
| 5 | result | UTF-8 `"1"` for success, `"0"` for service failure |

The v2 adapter maps the legacy request UUID to its internal request ID and
maps result `"1"` to success. Result `"0"` is a completed remote service
failure, not a transport timeout. Disconnect, invalid routing identity, or
decode failure is a transport error and must settle the pending request once.

## Discovery protocol

Discovery uses UDP datagrams sent to multicast group `239.255.0.7` by default.
Message discovery uses port `10317`; service discovery uses port `10318`.
They are independent channels and must not be merged.

Each datagram is exactly:

```text
u16_le(protobuf_payload_length) || gz.msgs.Discovery protobuf payload
```

The payload length excludes the two-byte prefix and must equal the received
datagram length minus two. The normal wire version is `10`; if
`GZ_TRANSPORT_TOPIC_STATISTICS=1`, it is `110`.

Supported `Discovery.type` values are `ADVERTISE`, `SUBSCRIBE`,
`UNADVERTISE`, `HEARTBEAT`, `BYE`, `NEW_CONNECTION`, and `END_CONNECTION`.
`ADVERTISE`, `UNADVERTISE`, `NEW_CONNECTION`, and `END_CONNECTION` carry a
publisher record. `SUBSCRIBE` carries only a topic. Heartbeat and bye carry no
contents. Publisher records include topic, endpoint, process UUID, node UUID,
scope, and either message metadata or service metadata.

The adapter preserves scopes `PROCESS`, `HOST`, and `ALL`. It must advertise
only non-`PROCESS` routes to remote peers; local process routing remains a
Node-level concern. The legacy timings are a 100 ms activity sweep, a 1000 ms
heartbeat, and a 3000 ms silence expiry.

### Environment configuration

| Variable | Meaning | Default |
| --- | --- | --- |
| `GZ_IP` | IPv4 interface used by discovery | detected host interface |
| `GZ_DISCOVERY_MULTICAST_IP` | discovery multicast group | `239.255.0.7` |
| `GZ_DISCOVERY_MSG_PORT` | message discovery port | `10317` |
| `GZ_DISCOVERY_SRV_PORT` | service discovery port | `10318` |
| `GZ_VERBOSE` | verbose discovery logging when `1` | disabled |
| `GZ_TRANSPORT_TOPIC_STATISTICS` | enables discovery wire version `110` when `1` | disabled |

If the configured message and service ports are equal, the service port must
be adjusted to a neighboring valid port, matching legacy behavior.

## Fixture contract

[`fixtures/legacy-compatibility.json`](fixtures/legacy-compatibility.json)
contains canonical fixture vectors. Future codec tests must decode every
fixture and must encode identical frame arrays or UDP datagrams. The fixture
payloads use documentation-only example identities and TEST-NET endpoints;
they contain no environment-specific addresses.

## Required interoperability matrix

The network suite must cover the following cases in both directions:

| Feature | New initiator | Legacy initiator | Expected result |
| --- | --- | --- | --- |
| PUB/SUB | new publisher -> legacy subscriber | legacy publisher -> new subscriber | topic, payload, endpoint, and message type retained |
| Discovery | new advertiser -> legacy subscriber | legacy advertiser -> new subscriber | route discovered with no manual endpoint |
| Service | new requester -> legacy service | legacy requester -> new service | correlated reply, success and service-failure mapping |
| Route loss | new peer loses legacy peer | legacy peer loses new peer | route withdrawn; pending work settles deterministically |
| Type mismatch | either side | either side | no handler dispatch; structured error/diagnostic |

Deterministic codec tests use the fixture file. Multicast and cross-process
ZeroMQ tests remain isolated network tests because they require host network
permissions.

## Non-goals

- Reintroducing legacy callback-based public APIs.
- Exposing legacy socket IDs, endpoints, or multipart frames through public
  v2 types.
- Preserving legacy's fixed readiness delay or its unbounded application
  queues.
