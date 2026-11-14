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
deployments must provide shared storage or consistent routing.

The runtime has bounded data-channel queues and flow control, without an arbitrary
bandwidth limit enabled by default. A bandwidth policy is opt-in.

## Development

Run pnpm check to typecheck, test, build ESM and CommonJS, and check all four
self-referencing package entrypoints.

Run pnpm smoke:browser to test a real Chromium-to-Werift roundtrip. Playwright
is a test-only dependency and is not included in the library artifact.

Package publication is intentionally disabled until separately authorized.
