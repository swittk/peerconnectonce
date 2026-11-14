# PeerConnectOnce

A generic peer-to-peer connection toolkit, independent of application frameworks and services.

**JavaScript output targets ES2018 (both ESM and CommonJS)**. This is a syntax/library target, not a promise that every ES2018-era runtime exposes WebRTC, Fetch or modern Web Crypto. The Node server's supported runtime floor remains Node.js 20; browser clients require native WebRTC or a supplied platform implementation.

Package: @peerconnectonce/core

Four public entry points:

- @peerconnectonce/core/client - platform-independent ICE/SDP signaling and reconnect lifecycle.
- @peerconnectonce/core/browser - native browser adapter; imports the shared client core.
- @peerconnectonce/core/server - Node.js/Werift peer server with registered service handlers.
- @peerconnectonce/core/signal - injectable rendezvous signaling with in-memory storage or a host-provided store.

Other runtimes such as React Native can supply their own WebRTC driver to /client.
All platform-specific runtime construction happens in adapters, never /client.

Authentication, node discovery, ICE/STUN/TURN policy and service authorization are supplied
by the embedding application. The core exposes no unrestricted LAN or file proxy.
Signaling carries short-lived offers and answers; application data goes via peer channels
or a TURN relay when needed. Signal storage is in-memory by default; multi-replica
deployments must provide shared storage or consistent routing. The memory store
enforces total, per-user pending, and per-user retained-session limits. A
terminal answer or failure is protected until its owner explicitly acknowledges
receipt via the signaling API or its retention deadline expires. The built-in
client acknowledges after the peer data channel opens. Capacity pressure evicts
only the oldest acknowledged terminal rows; unread answers and pending offers
are never stolen. If nothing is safe to reclaim, new offers are rejected with
a capacity error rather than disrupting existing negotiation.

The runtime has bounded data-channel queues and flow control, without an arbitrary
bandwidth limit enabled by default. A bandwidth policy is opt-in.

The Node server also guards **incoming Werift 0.27 SCTP fragments before reassembly**.
This is distinct from the completed-message queue limits: it checks the SCTP
receive window, an independent cumulative incomplete-message byte budget, and
per-message fragment and size limits. A violating peer is closed. This uses
Werift's version-specific receive interface, checked during session setup; if
an updated Werift release no longer exposes that interface, setup fails closed
instead of silently disabling protection. Any Werift upgrade requires re-running
the SCTP ingress tests and browser round-trip.

## Development

Run pnpm check to typecheck, test, build ESM and CommonJS, and check all four
self-referencing package entrypoints.

Run pnpm smoke:browser to test a real Chromium-to-Werift roundtrip. This
optional smoke test requires Playwright and a matching Chromium browser in
the test environment; neither is a package runtime dependency. Install
Playwright in an external QA project (for example: pnpm add -D playwright,
then pnpm exec playwright install chromium), and make it resolvable from
this repository or set PEERCONNECTONCE_PLAYWRIGHT to its absolute module path.

Package publication is intentionally disabled until separately authorized.
