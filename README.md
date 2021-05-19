# nats
A basic [Nats](http://nats.io) client library for Dlang, built on vibe.d

Aims to allow high-performance zero copy message processing, and minimal GC allocation
generated by the library itself in the "hot loop" (ie normal message receipt and processing).

For this reason, the `NatsHandlers` receive the `Msg` struct as a `scope` parameter,
and currently all message processing is handled synchronously, to avoid unnecessary
copying of `Msg` structs on the GC heap. This also matches with (my) typical deployment
profile of small, single purpose processes with a single message processing thread. 

If necessary for blocking IO or longer computations, you can copy the data required
from the `Msg` struct, and send to a task(fibre) or another thread. See example in the 
`query_responder` of the test app.

Current features supported:
- [x] Implement SUB api
- [x] Implement PUB api
- [x] Implement flush logic
- [x] Implement request-response subscriptions
- [x] Support proper connect options
- [x] Support reconnect logic
- [x] Support large messages
- [x] Support distributed queues (subscriber groups)
- [ ] Disconnect/clean shutdown
- [ ] Support Nats 2.0 Nkey-based authentication (requires libsodium for ed25519 keys)
- [ ] ? Support (de)serialisation protocols: msgpack, cerealed, none (passthru ubyte[])

For example usage, see the test app (`src/app.d`).
