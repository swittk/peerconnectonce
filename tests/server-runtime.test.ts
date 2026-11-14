import { describe, expect, it } from 'vitest';
import { RTCPeerConnection } from 'werift';
import { PeerServer } from '../src/server/index.js';

describe('real Werift server lifecycle', () => {
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
