# ADR 0010: gRPC data plane, REST admin/data, WebSocket bridge

**Status:** Accepted

## Context

Clients need efficient push delivery with backpressure; operators and scripts
need a plain HTTP surface; browsers cannot speak gRPC.

## Decision

- **gRPC** for the data plane, with a bidirectional `Receive` stream carrying
  server-pushed messages and client-streamed credits and settlements. Internal
  broker-to-broker forwarding uses the same transport.
- **REST** (minimal APIs + OpenAPI) for administration and a data-plane subset.
- **WebSocket** JSON frames mirroring the gRPC stream for browsers.
- One typed error model shared by all three.

## Consequences

- Credit-based flow control gives real backpressure and prefetch without polling.
- Three transports mean three surfaces to keep in sync; they are generated from
  and tested against one internal `IBrokerApi` so behaviour cannot diverge.
- Existing Service Bus AMQP clients cannot connect.

## Alternatives rejected

- **AMQP 1.0** — would allow existing clients, but implementing the broker side
  of AMQP is a project in itself and dwarfs the queue semantics.
- **REST only** — no server push; long polling wastes connections and latency.
