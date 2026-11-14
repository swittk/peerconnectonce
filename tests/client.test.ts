import { describe, expect, it } from 'vitest';
import {
  PeerClient, createHttpClientSignaling, type PeerClientSignaling, type PeerSessionStatus,
  type ClientPeerConnection, type PeerConnectionConfiguration,
} from '../src/client/index.js';

class FakeChannel extends EventTarget {
  readonly label: string;
  readyState: RTCDataChannelState = 'connecting';
  binaryType = 'blob';
  sent: string[] = [];
  constructor(label: string) { super(); this.label = label; }
  send(message: string) { this.sent.push(message); }
  open() { this.readyState = 'open'; this.dispatchEvent(new Event('open')); }
  close() { if (this.readyState === 'closed') return; this.readyState = 'closed'; this.dispatchEvent(new Event('close')); }
}
class FakePeer extends EventTarget {
  iceGatheringState: RTCIceGatheringState = 'new';
  connectionState: ClientPeerConnectionState = 'new';
  localDescription: RTCSessionDescription | null = null;
  channel?: FakeChannel;
  createDataChannel(label: string) { this.channel = new FakeChannel(label); return this.channel; }
  async createOffer() { return { type: 'offer' as const, sdp: 'fake-browser-offer' }; }
  async setLocalDescription(description: RTCSessionDescriptionInit) {
    this.localDescription = description as RTCSessionDescription;
    this.iceGatheringState = 'complete';
    this.dispatchEvent(new Event('icegatheringstatechange'));
  }
  async setRemoteDescription(_: RTCSessionDescriptionInit) {
    this.connectionState = 'connected';
    this.channel?.open();
  }
  close() {
    this.channel?.close();
    this.connectionState = 'closed';
    this.dispatchEvent(new Event('connectionstatechange'));
  }
}
describe('shared browser WebRTC connection lifecycle', () => {
  it('creates only one offer for simultaneous service requests and reconnects after channel close', async () => {
    const peers: FakePeer[] = [];
    const calls: string[] = [];
    const signaling: PeerClientSignaling = {
      getIceServers: async ({ protocol }) => { calls.push('ice:' + protocol); return []; },
      createOffer: async ({ protocol }) => { calls.push('offer:' + protocol); return { sessionId: 'id', iceServers: [] }; },
      readAnswer: async () => ({ status: 'answered', answerSdp: 'answer' }),
    };
    const client = new PeerClient({
      signaling, createPeerConnection: () => {
        const peer = new FakePeer();
        peers.push(peer);
        return peer as unknown as ClientPeerConnection;
      },
    });
    const [one, two] = await Promise.all([client.connect('echo-v1'), client.connect('echo-v1')]);
    expect(one).toBe(two);
    expect(calls).toEqual(['ice:echo-v1', 'offer:echo-v1']);
    expect(one.channel.readyState).toBe('open');
    one.close();
    const three = await client.connect('echo-v1');
    expect(three).not.toBe(one);
    expect(peers).toHaveLength(2);
    expect(calls.filter((call) => call.startsWith('offer'))).toHaveLength(2);
    client.close();
    expect(three.channel.readyState).toBe('closed');
    await expect(client.connect('echo-v1')).rejects.toThrow('peer_client_closed');
  });

  it('aborts only a joining caller without cancelling the shared negotiation', async () => {
    let releaseOffer: (() => void) | undefined;
    const heldOffer = new Promise<void>((resolve) => { releaseOffer = resolve; });
    let offers = 0;
    const client = new PeerClient({
      signaling: {
        getIceServers: async () => [],
        createOffer: async () => {
          offers++;
          await heldOffer;
          return { sessionId: 'shared', iceServers: [] };
        },
        readAnswer: async () => ({ status: 'answered', answerSdp: 'answer' }),
      },
      createPeerConnection: () => new FakePeer() as unknown as ClientPeerConnection,
    });
    const first = client.connect('echo-v1');
    const stopJoiner = new AbortController();
    const joined = client.connect('echo-v1', { signal: stopJoiner.signal });
    stopJoiner.abort();
    await expect(joined).rejects.toThrow('peer_aborted');
    releaseOffer?.();
    const connected = await first;
    expect(connected.channel.readyState).toBe('open');
    expect(offers).toBe(1);
    client.close();
  });

  it('cancels a late offer returned after the connection deadline', async () => {
    let releaseOffer: ((value: { sessionId: string; iceServers: [] }) => void) | undefined;
    let offerEntered: (() => void) | undefined;
    const entered = new Promise<void>((resolve) => { offerEntered = resolve; });
    const heldOffer = new Promise<{ sessionId: string; iceServers: [] }>((resolve) => { releaseOffer = resolve; });
    const cancelled: string[] = [];
    let wasCancelled: (() => void) | undefined;
    const cancelledOnce = new Promise<void>((resolve) => { wasCancelled = resolve; });
    const client = new PeerClient({
      connectionTimeoutMs: 300,
      signaling: {
        getIceServers: async () => [],
        createOffer: () => { offerEntered?.(); return heldOffer; },
        readAnswer: async () => ({ status: 'pending' }),
        cancelOffer: async (id) => { cancelled.push(id); wasCancelled?.(); },
      },
      createPeerConnection: () => new FakePeer() as unknown as ClientPeerConnection,
    });
    const connecting = client.connect('echo-v1');
    await entered;
    await expect(connecting).rejects.toThrow('peer_timeout:signal_offer');
    releaseOffer?.({ sessionId: 'late-offer', iceServers: [] });
    await cancelledOnce;
    expect(cancelled).toEqual(['late-offer']);
    client.close();
  });

  it('snapshots mutable ICE URL arrays before comparing late preflight changes', async () => {
    const urls = ['stun:old.example'];
    const configurations: PeerConnectionConfiguration[] = [];
    let offers = 0;
    const client = new PeerClient({
      signaling: {
        getIceServers: async () => [{ urls }],
        createOffer: async () => {
          offers++;
          urls[0] = 'turn:new.example';
          return { sessionId: 'mutable-' + offers, iceServers: [{ urls }] };
        },
        cancelOffer: async () => undefined,
        readAnswer: async () => ({ status: 'answered', answerSdp: 'answer' }),
      },
      createPeerConnection: (config) => {
        configurations.push(config);
        return new FakePeer() as unknown as ClientPeerConnection;
      },
    });
    const handle = await client.connect('echo-v1');
    expect(handle.channel.readyState).toBe('open');
    expect(offers).toBe(2);
    expect(configurations[0]!.iceServers).toEqual([{ urls: ['stun:old.example'] }]);
    expect(configurations[1]!.iceServers).toEqual([{ urls: ['turn:new.example'] }]);
    client.close();
  });

  it('preserves a stateful signaling instance as the acknowledgement receiver', async () => {
    class StatefulSignaling implements PeerClientSignaling {
      readonly received: string[] = [];
      getIceServers = async () => [];
      createOffer = async () => ({ sessionId: 'stateful', iceServers: [] });
      readAnswer = async (): Promise<PeerSessionStatus> => ({ status: 'answered', answerSdp: 'answer' });
      async acknowledgeOffer(id: string) {
        this.received.push(id);
      }
    }
    const signaling = new StatefulSignaling();
    const client = new PeerClient({
      signaling,
      createPeerConnection: () => new FakePeer() as unknown as ClientPeerConnection,
    });
    await client.connect('echo-v1');
    await new Promise((resolve) => setImmediate(resolve));
    expect(signaling.received).toEqual(['stateful']);
    client.close();
  });

  it('acknowledges a session only after its data channel has opened', async () => {
    const acknowledgedIds: string[] = [];
    let finishAcknowledgement: (() => void) | undefined;
    const acknowledged = new Promise<void>((resolve) => { finishAcknowledgement = resolve; });
    const client = new PeerClient({
      signaling: {
        getIceServers: async () => [],
        createOffer: async () => ({ sessionId: 'received-answer', iceServers: [] }),
        readAnswer: async () => ({ status: 'answered', answerSdp: 'answer' }),
        acknowledgeOffer: async (id) => {
          acknowledgedIds.push(id);
          finishAcknowledgement?.();
        },
      },
      createPeerConnection: () => new FakePeer() as unknown as ClientPeerConnection,
    });
    const connected = await client.connect('echo-v1');
    expect(connected.channel.readyState).toBe('open');
    await acknowledged;
    expect(acknowledgedIds).toEqual(['received-answer']);
    client.close();
  });

  it('never acknowledges a failed connection and releases its signaling session', async () => {
    let ackCount = 0;
    let cancelCount = 0;
    let reportCancellation: (() => void) | undefined;
    const cancelled = new Promise<void>((resolve) => { reportCancellation = resolve; });
    class FailingPeer extends FakePeer {
      async setRemoteDescription(_description: RTCSessionDescriptionInit) {
        throw new Error('invalid_remote_answer');
      }
    }
    const client = new PeerClient({
      signaling: {
        getIceServers: async () => [],
        createOffer: async () => ({ sessionId: 'bad-answer', iceServers: [] }),
        readAnswer: async () => ({ status: 'answered', answerSdp: 'answer' }),
        acknowledgeOffer: async () => { ackCount++; },
        cancelOffer: async (id) => {
          expect(id).toBe('bad-answer');
          cancelCount++;
          reportCancellation?.();
        },
      },
      createPeerConnection: () => new FailingPeer() as unknown as ClientPeerConnection,
    });
    await expect(client.connect('echo-v1')).rejects.toThrow('invalid_remote_answer');
    await cancelled;
    expect(ackCount).toBe(0);
    expect(cancelCount).toBe(1);
    client.close();
  });

  it('does not publish or acknowledge a connection aborted immediately after its channel opens', async () => {
    let client: PeerClient;
    let acknowledged = 0;
    let cancelled = 0;
    let offered: FakeChannel | undefined;
    class RaceChannel extends FakeChannel {
      open() {
        super.open();
        // The open event has already satisfied the pending wait. Shutdown
        // synchronously before the await continuation can publish the handle.
        client.close();
      }
    }
    class RacePeer extends FakePeer {
      createDataChannel(label: string) {
        offered = new RaceChannel(label);
        this.channel = offered;
        return offered;
      }
      async setRemoteDescription(_description: RTCSessionDescriptionInit) {
        this.connectionState = 'connected';
        setTimeout(() => this.channel?.open(), 0);
      }
    }
    client = new PeerClient({
      signaling: {
        getIceServers: async () => [],
        createOffer: async () => ({ sessionId: 'aborted-after-open', iceServers: [] }),
        readAnswer: async () => ({ status: 'answered', answerSdp: 'answer' }),
        acknowledgeOffer: async () => { acknowledged++; },
        cancelOffer: async () => { cancelled++; },
      },
      createPeerConnection: () => new RacePeer() as unknown as ClientPeerConnection,
    });
    await expect(client.connect('echo-v1')).rejects.toThrow('peer_aborted');
    await Promise.resolve();
    expect(acknowledged).toBe(0);
    expect(offered?.readyState).toBe('closed');
    expect(cancelled).toBe(1);
  });

  it('observes a rejected injected operation that aborts during invocation', async () => {
    let client: PeerClient;
    class AbortingPeer extends FakePeer {
      async setLocalDescription(_description: RTCSessionDescriptionInit) {
        client.close();
        throw new Error('injected_local_rejection');
      }
    }
    client = new PeerClient({
      signaling: {
        getIceServers: async () => [],
        createOffer: async () => ({ sessionId: 'unused', iceServers: [] }),
        readAnswer: async () => ({ status: 'pending' }),
      },
      createPeerConnection: () => new AbortingPeer() as unknown as ClientPeerConnection,
    });
    await expect(client.connect('echo-v1')).rejects.toThrow('peer_aborted');
    // Vitest also fails the suite on an unhandled rejection from the injected
    // operation, so this checks both cancellation and rejection ownership.
  });

  it('rejects a channel that opens and closes in the same callback without acknowledging', async () => {
    let ackCount = 0;
    let cancelCount = 0;
    class BlinkChannel extends FakeChannel {
      open() {
        super.open();
        super.close();
      }
    }
    class BlinkPeer extends FakePeer {
      createDataChannel(label: string) {
        this.channel = new BlinkChannel(label);
        return this.channel;
      }
      async setRemoteDescription(_: RTCSessionDescriptionInit) {
        this.connectionState = 'connected';
        setTimeout(() => this.channel?.open(), 0);
      }
    }
    const client = new PeerClient({
      signaling: {
        getIceServers: async () => [],
        createOffer: async () => ({ sessionId: 'blink', iceServers: [] }),
        readAnswer: async () => ({ status: 'answered', answerSdp: 'answer' }),
        acknowledgeOffer: async () => { ackCount++; },
        cancelOffer: async () => { cancelCount++; },
      },
      createPeerConnection: () => new BlinkPeer() as unknown as ClientPeerConnection,
    });
    await expect(client.connect('echo-v1')).rejects.toThrow('peer_channel_failed');
    await Promise.resolve();
    expect(ackCount).toBe(0);
    expect(cancelCount).toBe(1);
    client.close();
  });

  it('retries failed acknowledgements and releases signaling capacity on persistent failure', async () => {
    const client = new PeerClient({
      signaling: {
        getIceServers: async () => [],
        createOffer: async () => ({ sessionId: 'ack-failure', iceServers: [] }),
        readAnswer: async () => ({ status: 'answered', answerSdp: 'answer' }),
        acknowledgeOffer: async () => { throw new Error('temporary_ack_failure'); },
        cancelOffer: async () => { released?.(); },
      },
      createPeerConnection: () => new FakePeer() as unknown as ClientPeerConnection,
    });
    let released: (() => void) | undefined;
    const releasedPromise = new Promise<void>((resolve) => { released = resolve; });
    const connected = await client.connect('echo-v1');
    expect(connected.channel.readyState).toBe('open');
    await Promise.race([
      releasedPromise,
      new Promise<never>((_resolve, reject) => setTimeout(() => reject(new Error('ack_release_timeout')), 2_500)),
    ]);
    expect(connected.channel.readyState).toBe('open');
    client.close();
  });

  it('retries an acknowledgement after transient rejection and avoids cancellation', async () => {
    let attempts = 0;
    let cancelled = 0;
    let acknowledged: (() => void) | undefined;
    const acked = new Promise<void>((resolve) => { acknowledged = resolve; });
    const client = new PeerClient({
      signaling: {
        getIceServers: async () => [],
        createOffer: async () => ({ sessionId: 'ack-recovered', iceServers: [] }),
        readAnswer: async () => ({ status: 'answered', answerSdp: 'answer' }),
        acknowledgeOffer: async () => {
          attempts++;
          if (attempts < 3) throw new Error('transient_error');
          acknowledged?.();
        },
        cancelOffer: async () => { cancelled++; },
      },
      createPeerConnection: () => new FakePeer() as unknown as ClientPeerConnection,
    });
    const connected = await client.connect('echo-v1');
    await acked;
    // The callback signals before the promise completion updates the client's
    // acknowledged flag; wait for that normal completion, not merely the call.
    await new Promise((resolve) => setImmediate(resolve));
    expect(attempts).toBe(3);
    expect(cancelled).toBe(0);
    client.close();
    await Promise.resolve();
    expect(cancelled).toBe(0);
    expect(connected.channel.readyState).toBe('closed');
  });

  it('rejects different service on one live client and cannot silently reuse an old channel', async () => {
    const client = new PeerClient({
      signaling: {
        getIceServers: async () => [],
        createOffer: async () => ({ sessionId: 'id', iceServers: [] }),
        readAnswer: async () => ({ status: 'answered', answerSdp: 'answer' }),
      },
      createPeerConnection: () => new FakePeer() as unknown as ClientPeerConnection,
    });
    await client.connect('read-v1');
    await expect(client.connect('upload-v1')).rejects.toThrow('peer_client_in_use');
    client.close();
  });

  it('does not reuse an in-flight or active channel for a different target node', async () => {
    const offeredNodes: string[] = [];
    let resumeFirstOffer: (() => void) | undefined;
    const slowOffer = new Promise<void>((resolve) => { resumeFirstOffer = resolve; });
    const client = new PeerClient({
      signaling: {
        getIceServers: async () => [],
        createOffer: async ({ nodeId }) => {
          offeredNodes.push(nodeId ?? 'default');
          await slowOffer;
          return { sessionId: 'id', iceServers: [] };
        },
        readAnswer: async () => ({ status: 'answered', answerSdp: 'answer' }),
      },
      createPeerConnection: () => new FakePeer() as unknown as ClientPeerConnection,
    });
    const pending = client.connect('echo-v1', { nodeId: 'node-a' });
    await expect(client.connect('echo-v1', { nodeId: 'node-b' })).rejects.toThrow('peer_client_in_use');
    resumeFirstOffer?.();
    await pending;
    await expect(client.connect('echo-v1', { nodeId: 'node-b' })).rejects.toThrow('peer_client_in_use');
    expect(offeredNodes).toEqual(['node-a']);
    client.close();
    const alreadyAborted = new AbortController();
    alreadyAborted.abort();
    const second = new PeerClient({
      signaling: { getIceServers: async () => [], createOffer: async () => ({ sessionId: 'id', iceServers: [] }), readAnswer: async () => ({ status: 'answered', answerSdp: 'answer' }) },
      createPeerConnection: () => new FakePeer() as unknown as ClientPeerConnection,
    });
    await expect(second.connect('echo-v1', { signal: alreadyAborted.signal })).rejects.toThrow('peer_aborted');
    second.close();
  });

  it('reoffers once when a legacy signaling host supplies TURN only after the first offer', async () => {
    const rtcConfigs: PeerConnectionConfiguration[] = [];
    let offered = 0;
    let cancelled = 0;
    const client = new PeerClient({
      signaling: {
        getIceServers: async () => [],
        createOffer: async () => {
          offered++;
          return { sessionId: 'id-' + offered, iceServers: [{ urls: 'turn:node.example:3478' }] };
        },
        readAnswer: async () => ({ status: 'answered', answerSdp: 'answer' }),
        cancelOffer: async () => { cancelled++; },
      },
      createPeerConnection: (config) => {
        rtcConfigs.push(config);
        return new FakePeer() as unknown as ClientPeerConnection;
      },
    });
    const connected = await client.connect('echo-v1');
    expect(connected.channel.readyState).toBe('open');
    expect(offered).toBe(2);
    expect(cancelled).toBe(1);
    expect(rtcConfigs[0]!.iceServers).toEqual([]);
    expect(rtcConfigs[1]!.iceServers).toEqual([{ urls: 'turn:node.example:3478' }]);
    client.close();
  });

  it('cancels unread HTTP error bodies without masking the response status', async () => {
    let cancelled = 0;
    const signaling = createHttpClientSignaling({
      baseUrl: 'https://broker.example/peer',
      headers: () => ({}),
      fetch: async () => new Response(new ReadableStream({
        cancel() { cancelled++; },
      }), { status: 503 }),
    });
    await expect(signaling.readAnswer('session')).rejects.toThrow('peer_signal_http_503');
    expect(cancelled).toBe(1);
  });

  it('rejects malformed HTTP answer states without polling indefinitely', async () => {
    const malformed = [
      { status: 'answered' },
      { status: 'answered', answerSdp: '' },
      { status: 'answered', answerSdp: 42 },
      { status: 'pending', answerSdp: 'should-not-appear' },
      { status: 'nonsense', answerSdp: 'answer' },
      [],
      null,
    ];
    for (const body of malformed) {
      let reads = 0;
      const signaling = createHttpClientSignaling({
        baseUrl: 'https://broker.example/peer',
        headers: () => ({}),
        fetch: async () => {
          reads++;
          return new Response(JSON.stringify(body), {
            status: 200, headers: { 'content-type': 'application/json' },
          });
        },
      });
      await expect(signaling.readAnswer('session')).rejects.toThrow('invalid_peer_answer');
      expect(reads).toBe(1);
    }
    for (const body of [
      { status: 'pending' },
      { status: 'failed' },
      { status: 'answered', answerSdp: 'valid-sdp' },
    ]) {
      const signaling = createHttpClientSignaling({
        baseUrl: 'https://broker.example/peer',
        headers: () => ({}),
        fetch: async () => new Response(JSON.stringify(body), { status: 200 }),
      });
      expect(await signaling.readAnswer('session')).toEqual(body);
    }
  });

  it('rejects malformed injected signaling states without a busy retry loop', async () => {
    let reads = 0;
    const client = new PeerClient({
      signaling: {
        getIceServers: async () => [],
        createOffer: async () => ({ sessionId: 'malformed', iceServers: [] }),
        readAnswer: async () => {
          reads++;
          return { status: 'answered' } as unknown as PeerSessionStatus;
        },
      },
      createPeerConnection: () => new FakePeer() as unknown as ClientPeerConnection,
    });
    await expect(client.connect('echo-v1')).rejects.toThrow('invalid_peer_answer');
    expect(reads).toBe(1);
    client.close();
  });

  it('cancels stalled client credential generation without issuing a request', async () => {
    const controller = new AbortController();
    let fetched = 0;
    let started: (() => void) | undefined;
    const headersStarted = new Promise<void>((resolve) => { started = resolve; });
    let release: (() => void) | undefined;
    const headers = new Promise<Record<string, string>>((resolve) => {
      release = () => resolve({ authorization: 'Bearer stale' });
    });
    const signaling = createHttpClientSignaling({
      baseUrl: 'https://broker.example/peer',
      headers: () => { started?.(); return headers; },
      fetch: async () => {
        fetched++;
        return new Response('{}');
      },
    });
    const pending = signaling.getIceServers({ protocol: 'echo-v1', signal: controller.signal });
    await headersStarted;
    controller.abort();
    await expect(pending).rejects.toThrow('peer_request_aborted');
    release?.();
    await Promise.resolve();
    expect(fetched).toBe(0);
  });

  it('fences the client HTTP dispatch if headers resolve just before shutdown', async () => {
    const controller = new AbortController();
    let fetched = 0;
    const signaling = createHttpClientSignaling({
      baseUrl: 'https://broker.example/peer',
      headers: () => {
        queueMicrotask(() => queueMicrotask(() => controller.abort()));
        return {};
      },
      fetch: async () => {
        fetched++;
        return new Response('{}');
      },
    });
    await expect(signaling.getIceServers({ protocol: 'echo-v1', signal: controller.signal }))
      .rejects.toThrow('peer_request_aborted');
    expect(fetched).toBe(0);
  });

  it('accepts old signaling 404/501 ICE preflight without bypassing offer authorization', async () => {
    for (const status of [404, 501]) {
      const signaling = createHttpClientSignaling({
        baseUrl: 'https://node.example/peer',
        headers: () => ({ 'authorization': 'staff-token' }),
        fetch: async (input, init) => {
          expect(String(input)).toContain('/ice-config?protocol=echo-v1');
          expect(new Headers(init?.headers).get('authorization')).toBe('staff-token');
          return new Response(null, { status });
        },
      });
      expect(await signaling.getIceServers({ protocol: 'echo-v1' })).toEqual([]);
    }
  });

  it('keeps omitted node discovery distinct from explicit default node in pending and active reuse', async () => {
    let unblock: (() => void) | undefined;
    const holdOffer = new Promise<void>((resolve) => { unblock = resolve; });
    const seen: (string | undefined)[] = [];
    const client = new PeerClient({
      signaling: {
        getIceServers: async () => [],
        createOffer: async ({ nodeId }) => {
          seen.push(nodeId);
          await holdOffer;
          return { sessionId: 'offer', iceServers: [] };
        },
        readAnswer: async () => ({ status: 'answered', answerSdp: 'answer' }),
      },
      createPeerConnection: () => new FakePeer() as unknown as ClientPeerConnection,
    });
    const pending = client.connect('read-v1');
    await expect(client.connect('read-v1', { nodeId: 'default' })).rejects.toThrow('peer_client_in_use');
    unblock?.();
    await pending;
    await expect(client.connect('read-v1', { nodeId: 'default' })).rejects.toThrow('peer_client_in_use');
    expect(seen).toEqual([undefined]);
    client.close();
  });

  it('bounds a stalled late-ICE cancel within the original connection deadline and aborts DELETE', async () => {
    let wasAborted = false;
    let offerCount = 0;
    const client = new PeerClient({
      // Leave enough headroom for parallel test scheduling before the targeted
      // stalled DELETE; the timeout must be exercised by late-ICE cancellation.
      connectionTimeoutMs: 350,
      signaling: {
        getIceServers: async () => [],
        createOffer: async () => {
          offerCount++;
          return { sessionId: 'late-ice-offer', iceServers: [{ urls: 'turn:node.example:3478' }] };
        },
        readAnswer: async () => ({ status: 'pending' }),
        cancelOffer: async (_id, signal) => {
          signal?.addEventListener('abort', () => { wasAborted = true; }, { once: true });
          return new Promise<void>(() => undefined);
        },
      },
      createPeerConnection: () => new FakePeer() as unknown as ClientPeerConnection,
    });
    const started = Date.now();
    await expect(client.connect('read-v1')).rejects.toThrow('peer_timeout:late_ice_cancel');
    expect(Date.now() - started).toBeLessThan(1_000);
    expect(wasAborted).toBe(true);
    expect(offerCount).toBe(1); // timed-out cancellation must not start a second offer
    client.close();
  });

  it('does not leave an indefinite pending negotiation when the node never answers', async () => {
    let cancelCount = 0;
    const client = new PeerClient({
      connectionTimeoutMs: 50, answerPollIntervalMs: 10,
      signaling: {
        getIceServers: async () => [],
        createOffer: async () => ({ sessionId: 'timeout-id', iceServers: [] }),
        readAnswer: async () => ({ status: 'pending' }),
        cancelOffer: async () => { cancelCount++; },
      },
      createPeerConnection: () => new FakePeer() as unknown as ClientPeerConnection,
    });
    await expect(client.connect('echo-v1')).rejects.toThrow('peer_timeout');
    await new Promise((resolve) => setTimeout(resolve, 0));
    expect(cancelCount).toBe(1);
    client.close();
  });
});
