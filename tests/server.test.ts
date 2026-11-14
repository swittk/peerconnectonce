import { describe, expect, it } from 'vitest';
import { BoundedPeerWriter, PeerServiceRegistry, PeerServer, createHttpNodeSignaling } from '../src/server/index.js';

function channel() {
  let sendCount = 0;
  const sent: (string | Buffer)[] = [];
  let onLow = () => {};
  let subscribed = false;
  let onSubscribe: (() => void) | undefined;
  let bufferedAmount = 0;
  return {
    get readyState() { return 'open' as const; },
    get bufferedAmount() { return bufferedAmount; },
    set bufferedAmount(bytes: number) { bufferedAmount = bytes; },
    bufferedAmountLowThreshold: 0,
    bufferedAmountLow: { subscribe(fn: () => void) {
      onLow = fn;
      subscribed = true;
      onSubscribe?.();
      onSubscribe = undefined;
      return { unSubscribe() { onLow = () => {}; } };
    } },
    whenWaiting(): Promise<void> {
      if (subscribed) return Promise.resolve();
      return new Promise<void>((resolve) => { onSubscribe = resolve; });
    },
    send(data: string | Buffer) { sent.push(data); sendCount++; },
    close() {},
    emitLow() { onLow(); },
    get sent() { return sent; },
    get sendCount() { return sendCount; },
  };
}

describe('bounded HTTP node signaling', () => {
  it('never calls fetch if shutdown lands after header resolution but before dispatch', async () => {
    const shutdown = new AbortController();
    let fetches = 0;
    const node = createHttpNodeSignaling({
      baseUrl: 'https://broker.example/peer',
      nodeId: 'node-a',
      headers: () => {
        // The header promise can resolve before this nested microtask abort.
        // Check again immediately before dispatching the network request.
        queueMicrotask(() => queueMicrotask(() => shutdown.abort()));
        return {};
      },
      fetch: async () => {
        fetches++;
        return new Response(null, { status: 204 });
      },
    });
    await expect(node.claim(shutdown.signal)).rejects.toThrow('peer_request_aborted');
    expect(shutdown.signal.aborted).toBe(true);
    expect(fetches).toBe(0);
  });

  it('times out asynchronous credential headers before calling fetch', async () => {
    let fetches = 0;
    const node = createHttpNodeSignaling({
      baseUrl: 'https://broker.example/peer',
      nodeId: 'node-a',
      requestTimeoutMs: 40,
      headers: () => new Promise<Record<string, string>>(() => undefined),
      fetch: async () => {
        fetches++;
        return new Response(null, { status: 204 });
      },
    });
    const started = Date.now();
    await expect(node.claim()).rejects.toThrow('peer_request_aborted');
    expect(fetches).toBe(0);
    expect(Date.now() - started).toBeLessThan(400);
  });

  it('does not send a request when shutdown aborts async header generation', async () => {
    let startHeaders: (() => void) | undefined;
    const headersStarted = new Promise<void>((resolve) => { startHeaders = resolve; });
    let releaseHeaders: (() => void) | undefined;
    const stalledHeaders = new Promise<Record<string, string>>((resolve) => {
      releaseHeaders = () => resolve({ authorization: 'Bearer stale' });
    });
    let fetches = 0;
    const stop = new AbortController();
    const node = createHttpNodeSignaling({
      baseUrl: 'https://broker.example/peer',
      nodeId: 'node-a',
      requestTimeoutMs: 30_000,
      headers: () => { startHeaders?.(); return stalledHeaders; },
      fetch: async () => {
        fetches++;
        return new Response(null, { status: 204 });
      },
    });
    const pending = node.claim(stop.signal);
    await headersStarted;
    stop.abort();
    await expect(pending).rejects.toThrow('peer_request_aborted');
    releaseHeaders?.();
    await Promise.resolve();
    expect(fetches).toBe(0);
  });

  it('cancels a response body stalled after headers on timeout', async () => {
    let aborted = false;
    const node = createHttpNodeSignaling({
      baseUrl: 'https://broker.example/peer',
      nodeId: 'node-a',
      headers: () => ({ authorization: 'Bearer test-only' }),
      requestTimeoutMs: 50,
      fetch: async (_url, init) => new Response(new ReadableStream({
        start(controller) {
          init?.signal?.addEventListener('abort', () => {
            aborted = true;
            controller.error(new Error('body_aborted'));
          }, { once: true });
        },
      }), { status: 200, headers: { 'content-type': 'application/json' } }),
    });
    const started = Date.now();
    await expect(node.claim()).rejects.toThrow('body_aborted');
    expect(aborted).toBe(true);
    expect(Date.now() - started).toBeLessThan(400);
  });

  it('propagates shutdown to a stalled claim body, not only to its headers', async () => {
    const shutdown = new AbortController();
    const node = createHttpNodeSignaling({
      baseUrl: 'https://broker.example/peer',
      nodeId: 'node-a',
      headers: () => ({}),
      requestTimeoutMs: 20_000,
      fetch: async (_url, init) => new Response(new ReadableStream({
        start(controller) {
          init?.signal?.addEventListener('abort', () => {
            controller.error(new Error('shutdown_aborted_body'));
          }, { once: true });
        },
      }), { status: 200 }),
    });
    const pending = node.claim(shutdown.signal);
    setTimeout(() => shutdown.abort(), 15);
    await expect(pending).rejects.toThrow('shutdown_aborted_body');
  });
});

describe('generic peer service registry', () => {
  it('requires an explicitly registered reliable service', () => {
    const registry = new PeerServiceRegistry([
      { label: 'echo-v1', create: () => ({ receiveMessage() {}, close() {} }) },
    ]);
    expect(registry.has('echo-v1')).toBe(true);
    expect(registry.has('read-v1')).toBe(false);
    const good = { label: 'echo-v1', ordered: true, maxRetransmits: null, maxPacketLifeTime: null };
    expect(registry.create(good as never, {
      ownerUserId: 'alice', protocol: 'echo-v1', nodeId: 'node1',
      signal: new AbortController().signal,
      writer: { sendText: async () => {}, sendBinary: async () => {}, close() {} },
      beginWork: () => () => {},
    })).toBeDefined();
    for (const wrong of [
      { ...good, label: 'nope' },
      { ...good, ordered: false },
      { ...good, maxRetransmits: 1 },
    ]) expect(() => registry.create(wrong as never, {
      ownerUserId: 'alice', protocol: 'echo-v1', nodeId: 'node1',
      signal: new AbortController().signal,
      writer: { sendText: async () => {}, sendBinary: async () => {}, close() {} },
      beginWork: () => () => {},
    })).toThrow('unregistered_or_unreliable_peer_channel');
  });
});

describe('bounded resource configuration', () => {
  const base = {
    signaling: { claim: async () => undefined, answer: async () => undefined, fail: async () => undefined },
    services: [{ label: 'echo-v1', create: () => ({ receiveMessage() {}, close() {} }) }],
    iceServers: [],
  };
  it('rejects non-finite, fractional and invalid peer limits before starting', () => {
    for (const invalid of [
      { maxPeers: NaN }, { maxPeers: Infinity }, { maxPeers: 0 },
      { pollIntervalMs: NaN }, { setupTimeoutMs: Infinity }, { idleTimeoutMs: 0.5 },
      { bufferedHighBytes: NaN }, { bufferedLowBytes: NaN },
      { maxQueuedBytes: Infinity }, { maxQueuedMessages: NaN }, { maxMessageBytes: NaN },
    ]) {
      expect(() => new PeerServer({ ...base, ...invalid })).toThrow(/invalid_peer/);
    }
  });
  it('rejects invalid writer capacity instead of silently disabling backpressure', () => {
    for (const invalid of [
      { highBytes: NaN }, { highBytes: Infinity }, { highBytes: 1.5 },
      { lowBytes: NaN }, { lowBytes: -1 },
      { maxQueuedBytes: NaN }, { maxQueuedBytes: 0 },
      { maxMessageBytes: NaN }, { maxMessageBytes: 0.5 },
      { maxQueuedMessages: Infinity }, { outgoingBytesPerSecond: NaN },
    ]) {
      expect(() => new BoundedPeerWriter(channel() as never, invalid)).toThrow('invalid_peer_writer_limits');
    }
  });
});

describe('bounded writer, no artificial default throttle', () => {
  it('starts closed when supplied a previously-aborted signal', async () => {
    const signal = new AbortController();
    signal.abort();
    const fake = channel();
    const writer = new BoundedPeerWriter(fake as never, { signal: signal.signal });
    await expect(writer.sendText('hello')).rejects.toThrow('peer_channel_closed');
    expect(fake.sendCount).toBe(0);
  });

  it('does not retain large backing buffers or allow source mutation while queued', async () => {
    const fake = channel();
    fake.bufferedAmount = 200;
    const writer = new BoundedPeerWriter(fake as never, {
      highBytes: 100, lowBytes: 20, maxMessageBytes: 2048,
    });
    const backing = new Uint8Array(16 * 1024 * 1024);
    backing[10] = 87;
    const view = backing.subarray(10, 1034);
    const pending = writer.sendBinary(view);
    await fake.whenWaiting();
    backing[10] = 12;
    fake.bufferedAmount = 0;
    fake.emitLow();
    await pending;
    const sent = fake.sent[0] as Buffer;
    expect(sent[0]).toBe(87);
    expect(sent.length).toBe(1024);
    expect(sent.buffer.byteLength).toBeLessThan(1024 * 1024);
    writer.close();
  });
  it('can send many frames without per-frame timer work', async () => {
    const fake = channel();
    const writer = new BoundedPeerWriter(fake as never, { maxQueuedBytes: 24 * 1024 * 1024 });
    const frames = Array.from({ length: 1024 }, () => writer.sendBinary(new Uint8Array(16 * 1024)));
    await Promise.all(frames);
    expect(fake.sendCount).toBe(1024);
    writer.close();
  });
  it('bounds queued bytes while waiting for SCTP buffer drain', async () => {
    const fake = channel();
    fake.bufferedAmount = 200;
    const writer = new BoundedPeerWriter(fake as never, {
      highBytes: 100, lowBytes: 20, maxQueuedBytes: 50, maxMessageBytes: 50,
    });
    const first = writer.sendBinary(new Uint8Array(32));
    await expect(writer.sendBinary(new Uint8Array(32))).rejects.toThrow('peer_writer_capacity');
    // Wait for actual transport backpressure, not a fixed timeout or queue-cap side effect.
    await Promise.race([
      fake.whenWaiting(),
      first.then(() => { throw new Error('writer_bypassed_backpressure'); }),
    ]);
    expect(fake.sendCount).toBe(0);
    fake.bufferedAmount = 0;
    fake.emitLow();
    await first;
    expect(fake.sendCount).toBe(1);
    writer.close();
  });

  it('bounds empty frames by entry count, not only retained bytes', async () => {
    const fake = channel();
    fake.bufferedAmount = 200;
    const writer = new BoundedPeerWriter(fake as never, {
      highBytes: 100, lowBytes: 20, maxQueuedBytes: 1024,
      maxQueuedMessages: 2,
    });
    const a = writer.sendText('');
    const b = writer.sendBinary(new Uint8Array(0));
    await expect(writer.sendText('')).rejects.toThrow('peer_writer_capacity');
    writer.close();
    await expect(a).rejects.toThrow('peer_channel_closed');
    await expect(b).rejects.toThrow('peer_channel_closed');
  });

  it('aborts outstanding optional QoS pacing immediately when the writer closes', async () => {
    const fake = channel();
    const writer = new BoundedPeerWriter(fake as never, {
      outgoingBytesPerSecond: 1,
    });
    await writer.sendText('x');
    const second = writer.sendText('x');
    // A full second of artificial pacing is queued; close must break the delay.
    await new Promise((resolve) => setTimeout(resolve, 20));
    const start = Date.now();
    writer.close();
    await expect(second).rejects.toThrow('peer_channel_closed');
    expect(Date.now() - start).toBeLessThan(250);
  });

  it('refuses unbounded application queues without disabling actual buffer backpressure', async () => {
    const fake = channel();
    const writer = new BoundedPeerWriter(fake as never, { maxQueuedBytes: 40, maxMessageBytes: 32, highBytes: 80, lowBytes: 20 });
    expect(await writer.sendText('hello')).toBeUndefined();
    await expect(writer.sendBinary(new Uint8Array(33))).rejects.toThrow('peer_writer_capacity');
    writer.close();
  });
});
