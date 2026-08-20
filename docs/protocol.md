# Protocol

Dist-MQ speaks three protocols over two ports. They are thin adapters over one
`BrokerService`, so the semantics are identical and only the framing differs
(ADR 0010).

| Port | Protocol | Surface |
|---|---|---|
| 5000 | HTTP/1.1 | REST administration, HTTP data plane, WebSocket bridge |
| 5001 | HTTP/2 | gRPC data plane |

**Why two ports.** A cleartext port cannot carry both HTTP/1.1 and h2c: without
TLS there is no ALPN to negotiate with, so Kestrel's `Http1AndHttp2` serves
HTTP/1.1 only and every gRPC call fails with `HTTP_1_1_REQUIRED`. Behind
TLS-terminating ingress a single port would do; the split is what the broker
itself speaks. Both are configurable (`DistMq:HttpPort`, `DistMq:GrpcPort`).

## Error model

Every transport reports the same error codes, so a client written against one
recognises failures from another.

| Code | gRPC status | HTTP status | Meaning |
|---|---|---|---|
| `EntityNotFound` | `NOT_FOUND` | 404 | No such queue, topic or subscription |
| `EntityAlreadyExists` | `ALREADY_EXISTS` | 409 | Creation collided with an existing entity |
| `InvalidArgument` | `INVALID_ARGUMENT` | 400 | Malformed path, filter or option |
| `NotOwner` | `FAILED_PRECONDITION` | 421 | Another broker owns the partition; see the redirect |
| `LockLost` | `FAILED_PRECONDITION` | 409 | The peek-lock expired or belongs to another receiver |
| `SessionLockLost` | `FAILED_PRECONDITION` | 409 | The session lock expired or is held elsewhere |
| `MessageNotDeferred` | `FAILED_PRECONDITION` | 409 | The sequence number is not set aside |
| `SessionRequirementMismatch` | `INVALID_ARGUMENT` | 400 | Session id missing, or sessions used on a plain entity |
| `MessageSizeExceeded` | `INVALID_ARGUMENT` | 413 | Body over the accepted limit |
| `Throttled` | `RESOURCE_EXHAUSTED` | 429 | Storage is throttling; retry after the given delay |
| `Fenced` | `UNAVAILABLE` | 503 | The broker lost the partition mid-operation |

gRPC carries the code in the `distmq-error-code` trailer and, for `NotOwner`,
the owner's address in `distmq-redirect`. HTTP returns
`{"error": "...", "message": "...", "redirect": "..."}`. The SDK turns both back
into a `DistMqException` with the same `Code`, so callers never parse messages.

## gRPC — `distmq.v1.Messaging`

| Method | Purpose |
|---|---|
| `Send` | Append messages; returns a sequence number per message |
| `Receive` | Lock up to N messages, optionally waiting |
| `Settle` | Complete, abandon, dead-letter or defer, in batches |
| `RenewLock` | Extend a peek-lock |
| `Peek` | Read without locking |
| `ScheduleMessage` / `CancelScheduledMessage` | Future delivery, and calling it off |
| `ReceiveDeferred` | Lock messages that were set aside, by sequence number |
| `AcceptSession` | Take a session lock, by id or "any available" |
| `ReceiveSession` | The session's next message |
| `RenewSessionLock` / `ReleaseSession` | Keep or hand back a session |
| `GetSessionState` / `SetSessionState` | Read and write state belonging to the session |

A **sequence number** packs the partition id into its high bits, so settlement,
deferred receives and cancellation route to the right partition without a
lookup.

## REST

Routes follow the entity hierarchy, and a dead-letter queue is an ordinary
entity (ADR 0006), so it takes exactly the same operations.

```
POST   /admin/queues                                     create a queue
GET    /admin/queues/{name}                              describe
GET    /admin/queues/{name}/runtime                      live counts
DELETE /admin/queues/{name}
POST   /admin/topics                                     create a topic
POST   /admin/topics/{topic}/subscriptions               create a subscription
PUT    /admin/topics/{topic}/subscriptions/{sub}/rules    replace rules
GET    /admin/entities                                   list everything

POST /queues/{name}/messages                             send
POST /queues/{name}/messages/receive                     receive
POST /queues/{name}/messages/peek                        peek
POST /queues/{name}/messages/{complete|abandon|deadletter|defer}
POST /queues/{name}/messages/settle                      settle a batch
POST /queues/{name}/messages/renewlock
POST /queues/{name}/messages/schedule
POST /queues/{name}/messages/receivedeferred
POST /queues/{name}/sessions/{accept|receive|renewlock|release|state}
GET  /health/live   /health/ready
```

`/queues/{name}/$deadletterqueue/...` and
`/topics/{topic}/subscriptions/{sub}/...` carry the same message operations.
Bodies are base64 so any payload survives JSON.

## WebSocket

`GET /ws/{entity}` upgrades to a JSON frame stream. Delivery is **credit-based**:
the client says how many messages it can handle and the broker pushes up to that
many, so a slow consumer slows the flow rather than being buried — the same
backpressure the gRPC stream provides.

Client frames:

```jsonc
{"type": "credit",  "count": 10}
{"type": "send",    "messages": [{"body": "<base64>", "subject": "greeting"}]}
{"type": "settle",  "action": "Complete", "sequenceNumber": 12, "lockToken": "..."}
{"type": "renew",   "sequenceNumber": 12, "lockToken": "..."}
```

Server frames:

```jsonc
{"type": "message", "message": { ... }}
{"type": "sent",    "sequenceNumbers": [12]}
{"type": "settled", "sequenceNumber": 12, "settled": true, "error": ""}
{"type": "renewed", "lockedUntil": "..."}
{"type": "error",   "code": "LockLost", "message": "..."}
```

## Settlement semantics

- A settle succeeds only while the lock is valid. After expiry it fails with
  `LockLost` rather than quietly succeeding, because the message may already be
  with another receiver.
- `Abandon` returns the message immediately and raises its delivery count; past
  `MaxDeliveryCount` it is dead-lettered instead of redelivered.
- `Defer` sets the message aside and lets the cursor move on. Only
  `ReceiveDeferred` brings it back.
- Batch settlement is one call, one log append and one table transaction —
  capped at 100 operations by Table Storage, so larger batches are chunked.
