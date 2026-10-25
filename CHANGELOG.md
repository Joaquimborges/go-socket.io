# Changelog

All notable changes to this client. Versions follow [semver](https://semver.org).

## v1.1.0 — unreleased

Additive release: the API only grows, and a client built without the new options behaves as before, except for
the two visible changes listed first.

### Changed

- **`OnConnect` runs only once the server accepts the connection.** The client now waits up to 20 seconds for
  the server's `CONNECT` before counting the connection as established, and the backoff resets only then.
  Before, the backoff reset as soon as the WebSocket opened, even if the server then refused the connection.
- **A refused connection is reported.** A `CONNECT_ERROR`, such as a server middleware rejecting the
  credentials, reaches `OnReconnectAttempt` as a `*ConnectError` and is retried with backoff. Before, it was
  ignored: the client sat unconnected until the server's `connectTimeout` closed the transport, then repeated,
  without ever reporting why.

### Added

- `Reconnect()` ends the current session and connects again after the initial backoff. `OnDisconnect`
  receives `ErrReconnectRequested`.
- `WithReconnectBackoff(initial, max, jitter)` configures the wait between attempts. The default is unchanged:
  1s doubling to 30s, no jitter. `OnReconnectAttempt` reports the actual wait.
- `WithEventErrorHandler(fn)` keeps the session when a handler panics or a payload does not decode, and still
  sends the ack. Without it, such an error ends the session without an ack, as before.
- `ConnectError`, `ErrReconnectRequested` and `ErrInvalidOption`.
- `parser.Decoder.DecodeRaw`.

### Fixed

- An ack for an event whose session had ended could be sent on the next connection, with an id that
  connection never issued. A write failure while a handler ran could also leave the old read loop reading from
  the new connection. Each connection now has its own session state, and an ack only goes to the session its
  event arrived on.
- The encoder omitted the separator between a custom namespace and an ack id of 0 (`3/ns0` instead of
  `3/ns,0`). The client only uses the root namespace, so this was latent.

### Upgrading

No code change is needed. Check whether anything depends on the old timing of `OnConnect`, and expect
`OnReconnectAttempt` to start reporting refusals if your server rejects the credentials.

## v1.0.0 — June 2026

Client-only fork: Engine.IO v4 over WebSocket, PING → PONG, automatic reconnect with exponential backoff,
`WithHeaders`, typed handlers and an empty acknowledgement of server events.
