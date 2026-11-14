import { describe, expect, it } from 'vitest';
import { RTCPeerConnection } from 'werift';
import { PeerServer } from '../src/server/index.js';

describe('real Werift server lifecycle', () => {
  it('rejects an already-aborted shutdown before allocating or publishing a peer', async () => {
    const shutdown = new AbortController();
    shutdown.abort();
    let answers = 0;
    const server = new PeerServer({
      iceServers: [],
      services: [{ label: 'echo-v1', create: () => ({ receiveMessage() {}, close() {} }) }],
      signaling: {
        claim: async () => undefined,
        answer: async () => { answers++; },
        fail: async () => undefined,
      },
    });
    await expect(server.accept({
      sessionId: 'aborted', nodeId: 'node-a', ownerUserId: 'user-a',
      protocol: 'echo-v1', offerSdp: 'would-not-parse',
    }, shutdown.signal)).rejects.toThrow('peer_setup_stopped');
    expect(answers).toBe(0);
  });

  it('waits for an already-started asynchronous service cleanup before start resolves', async () => {
    const browser = new RTCPeerConnection({ iceServers: [], iceUseDefaultStunServer: false });
    const channel = browser.createDataChannel('echo-v1', { ordered: true });
    await browser.setLocalDescription(await browser.createOffer());
    const offerSdp = browser.localDescription?.sdp;
    if (!offerSdp) throw new Error('missing_test_offer');

    let startedCleanup: (() => void) | undefined;
    const cleanupStarted = new Promise<void>((resolve) => { startedCleanup = resolve; });
    let finishCleanup: (() => void) | undefined;
    const cleanupWait = new Promise<void>((resolve) => { finishCleanup = resolve; });
    const shutdown = new AbortController();
    let didClaim = false;
    const server = new PeerServer({
      iceServers: [],
      pollIntervalMs: 10,
      idleTimeoutMs: 30_000,
      services: [{ label: 'echo-v1', create: () => ({
        receiveMessage() {},
        close: () => {
          startedCleanup?.();
          return cleanupWait;
        },
      }) }],
      signaling: {
        claim: async () => {
          if (didClaim) return undefined;
          didClaim = true;
          return {
            sessionId: 'lifecycle', nodeId: 'node-a', ownerUserId: 'user-a',
            protocol: 'echo-v1', offerSdp,
          };
        },
        answer: async (_sessionId, answerSdp) => {
          await browser.setRemoteDescription({ type: 'answer', sdp: answerSdp });
        },
        fail: async () => undefined,
      },
    });
    const running = server.start(shutdown.signal);
    try {
      const opened = new Promise<void>((resolve) => {
        if (channel.readyState === 'open') resolve();
        else channel.stateChanged.subscribe((state) => {
          if (state === 'open') resolve();
        });
      });
      await Promise.race([
        opened,
        new Promise<never>((_resolve, reject) =>
          setTimeout(() => reject(new Error('test_channel_not_open')), 6_000)),
      ]);
      shutdown.abort();
      await cleanupStarted;
      let didResolve = false;
      void running.then(() => { didResolve = true; });
      await new Promise((resolve) => setTimeout(resolve, 20));
      expect(didResolve).toBe(false);
      finishCleanup?.();
      await running;
      expect(didResolve).toBe(true);
    } finally {
      shutdown.abort();
      finishCleanup?.();
      await running.catch(() => undefined);
      await browser.close();
    }
  });

  it('observes a rejected signaling promise when the operation aborts shutdown', async () => {
    const browser = new RTCPeerConnection({ iceServers: [], iceUseDefaultStunServer: false });
    browser.createDataChannel('echo-v1', { ordered: true });
    await browser.setLocalDescription(await browser.createOffer());
    const sdp = browser.localDescription?.sdp;
    if (!sdp) throw new Error('missing_test_offer');
    const shutdown = new AbortController();
    let published = 0;
    const server = new PeerServer({
      iceServers: [], setupTimeoutMs: 3_000,
      services: [{ label: 'echo-v1', create: () => ({ receiveMessage() {}, close() {} }) }],
      signaling: {
        claim: async () => undefined,
        answer: () => {
          published++;
          shutdown.abort();
          // The original eager beforeDeadline could reject on the abort and
          // leave this separate injected promise rejection unobserved.
          return Promise.reject(new Error('injected_answer_rejected'));
        },
        fail: async () => undefined,
      },
    });
    try {
      await expect(server.accept({
        sessionId: 'abort-answer', nodeId: 'node-a',
        ownerUserId: 'user-a', protocol: 'echo-v1', offerSdp: sdp,
      }, shutdown.signal)).rejects.toThrow('peer_stopped');
      expect(published).toBe(1);
      // The test runner also fails if injected_answer_rejected leaks as
      // an unhandled rejection on the next microtask.
      await Promise.resolve();
    } finally {
      shutdown.abort();
      await browser.close();
    }
  });

  it('enforces maxPeers across concurrent direct accepts and outbound polling', async () => {
    const browser = new RTCPeerConnection({ iceServers: [], iceUseDefaultStunServer: false });
    browser.createDataChannel('echo-v1', { ordered: true });
    await browser.setLocalDescription(await browser.createOffer());
    const sdp = browser.localDescription?.sdp;
    if (!sdp) throw new Error('test_offer_missing');
    const lifetime = new AbortController();
    let claims = 0;
    const server = new PeerServer({
      maxPeers: 1, pollIntervalMs: 5, iceServers: [],
      services: [{ label: 'echo-v1', create: () => ({ receiveMessage() {}, close() {} }) }],
      signaling: {
        claim: async () => { claims++; return undefined; },
        answer: async () => undefined,
        fail: async () => undefined,
      },
    });
    const makeOffer = (sessionId: string) => ({
      sessionId, nodeId: 'node-a', ownerUserId: 'user-a',
      protocol: 'echo-v1', offerSdp: sdp,
    });
    try {
      const firstPending = server.accept(makeOffer('first'), lifetime.signal);
      await expect(server.accept(makeOffer('second'), lifetime.signal)).rejects.toThrow('peer_capacity');
      const first = await firstPending;
      await expect(server.accept(makeOffer('first'), lifetime.signal)).rejects.toThrow('peer_capacity');
      await expect(server.accept(makeOffer('second'), lifetime.signal)).rejects.toThrow('peer_capacity');
      const polling = server.start(lifetime.signal);
      try {
        await new Promise((resolve) => setTimeout(resolve, 35));
        expect(claims).toBe(0);
        await first.close();
        await new Promise((resolve) => setTimeout(resolve, 35));
        expect(claims).toBeGreaterThan(0);
      } finally {
        lifetime.abort();
        await polling;
      }
      const secondLifetime = new AbortController();
      const second = await server.accept(makeOffer('second'), secondLifetime.signal);
      await second.close();
    } finally {
      lifetime.abort();
      await browser.close();
    }
  });

  it('isolates a throwing activity probe instead of crashing the timer or other peers', async () => {
    const browser = new RTCPeerConnection({ iceServers: [], iceUseDefaultStunServer: false });
    const channel = browser.createDataChannel('echo-v1', { ordered: true });
    await browser.setLocalDescription(await browser.createOffer());
    const sdp = browser.localDescription?.sdp;
    if (!sdp) throw new Error('test_offer_missing');
    let probes = 0;
    let serviceClosed = 0;
    const stop = new AbortController();
    const server = new PeerServer({
      iceServers: [], idleTimeoutMs: 30,
      services: [{ label: 'echo-v1', create: () => ({
        receiveMessage() {},
        hasActiveWork() { probes++; throw new Error('adapter_activity_failed'); },
        close() { serviceClosed++; },
      }) }],
      signaling: {
        claim: async () => undefined,
        answer: async (_id, answerSdp) => {
          await browser.setRemoteDescription({ type: 'answer', sdp: answerSdp });
        },
        fail: async () => undefined,
      },
    });
    try {
      const peer = await server.accept({
        sessionId: 'activity', nodeId: 'node-a', ownerUserId: 'user-a',
        protocol: 'echo-v1', offerSdp: sdp,
      }, stop.signal);
      await Promise.race([
        peer.finished,
        new Promise<never>((_resolve, reject) => setTimeout(() => reject(new Error('activity_probe_did_not_close')), 5_000)),
      ]);
      if (channel.readyState !== 'closed') {
        await new Promise<void>((resolve, reject) => {
          let settled = false;
          const timeout = setTimeout(() => finish(false), 5_000);
          const subscription = channel.stateChanged.subscribe((state) => {
            if (state === 'closed') finish(true);
          });
          function finish(closed: boolean) {
            if (settled) return;
            settled = true;
            clearTimeout(timeout);
            subscription.unSubscribe();
            if (closed) resolve();
            else reject(new Error('remote_channel_not_closed'));
          }
          if (channel.readyState === 'closed') finish(true);
        });
      }
      expect(channel.readyState).toBe('closed');
      expect(probes).toBeGreaterThan(0);
      expect(serviceClosed).toBe(1);
    } finally {
      stop.abort();
      await browser.close();
    }
  });

  it('aborts a pending answer publish if this peer is closed during setup', async () => {
    const browser = new RTCPeerConnection({
      iceServers: [],
      iceUseDefaultStunServer: false,
    });
    const channel = browser.createDataChannel('echo-v1', { ordered: true });
    void channel;
    const offer = await browser.createOffer();
    await browser.setLocalDescription(offer);
    const sdp = browser.localDescription?.sdp;
    if (!sdp) throw new Error('test_offer_missing');
    const shutdown = new AbortController();
    let answerReached: (() => void) | undefined;
    const answerStarted = new Promise<void>((resolve) => { answerReached = resolve; });
    let sawAbort = false;
    const node = new PeerServer({
      iceServers: [], setupTimeoutMs: 5_000, idleTimeoutMs: 100,
      pollIntervalMs: 100,
      services: [{ label: 'echo-v1', create: () => ({
        receiveMessage() {},
        close() {},
      }) }],
      signaling: {
        claim: async () => undefined,
        async answer(_id, _sdp, signal) {
          answerReached?.();
          return new Promise<void>((_resolve, reject) => {
            const rejectAborted = () => {
              sawAbort = true;
              reject(new Error('answer_aborted'));
            };
            if (signal?.aborted) rejectAborted();
            else signal?.addEventListener('abort', rejectAborted, { once: true });
          });
        },
        fail: async () => undefined,
      },
    });
    const pending = node.accept({
      sessionId: 'session', nodeId: 'node-a', ownerUserId: 'user-a',
      protocol: 'echo-v1', offerSdp: sdp,
    }, shutdown.signal);
    try {
      await Promise.race([
        answerStarted,
        new Promise((_resolve, reject) => setTimeout(() => reject(new Error('answer_never_started')), 3_000)),
      ]);
      const startedAt = Date.now();
      shutdown.abort();
      await expect(pending).rejects.toThrow();
      expect(sawAbort).toBe(true);
      expect(Date.now() - startedAt).toBeLessThan(1_000);
    } finally {
      shutdown.abort();
      await browser.close();
    }
  });
});
