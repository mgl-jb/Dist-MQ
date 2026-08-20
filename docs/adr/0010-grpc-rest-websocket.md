# ADR 0010: gRPC data plane, REST admin/data, WebSocket bridge

**Status:** Accepted

## Context

Clients need efficient push delivery with backpressure; operators and scripts
need a plain HTTP surface; browsers cannot speak gRPC.

## Decision

- **gRPC** for the data plane, with a bidirectional `Receive` stream carrying
  server-pushed messages and client-streamed credits and settlements.
- **REST** (minimal APIs + OpenAPI) for administration and a data-plane subset.
- **WebSocket** JSON frames mirroring the gRPC stream for browsers.
- One typed error model shared by all three.

## Consequences

- Credit-based flow control gives real backpressure and prefetch without polling.
- Three transports mean three surfaces to keep in sync; all three are thin
  adapters over one `BrokerService`, so behaviour cannot diverge.
- A request that lands on a broker which does not own the partition is redirected
  rather than proxied: the error model carries the owner's endpoint and the client
  goes there directly, instead of every mis-addressed message paying two network
  hops for the life of the connection.
- Existing Service Bus AMQP clients cannot connect.

## Alternatives rejected

- **AMQP 1.0** — would allow existing clients, but implementing the broker side
  of AMQP is a project in itself and dwarfs the queue semantics.
- **REST only** — no server push; long polling wastes connections and latency.
