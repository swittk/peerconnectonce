import { describe, expect, it } from 'vitest';
import { BoundedPeerWriter, PeerServiceRegistry, createHttpNodeSignaling } from '../src/server/index.js';

function channel() {
  let sendCount = 0;
  const sent: (string | Buffer)[] = [];
  let onLow = () => {};
  let bufferedAmount = 0;
  return {
    get readyState() { return 'open' as const; },
    get bufferedAmount() { return bufferedAmount; },
    set bufferedAmount(bytes: number) { bufferedAmount = bytes; },
    bufferedAmountLowThreshold: 0,
    bufferedAmountLow: { subscribe(fn: () => void) {
      onLow = fn;
      return { unSubscribe() { onLow = () => {}; } };
    } },
    send(data: string | Buffer) { sent.push(data); sendCount++; },
    close() {},
    emitLow() { onLow(); },
    get sent() { return sent; },
    get sendCount() { return sendCount; },
  };
}

describe('bounded HTTP node signaling', () => {
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

describe('bounded writer, no artificial default throttle', () => {
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
