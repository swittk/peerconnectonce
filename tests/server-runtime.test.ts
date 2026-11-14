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
