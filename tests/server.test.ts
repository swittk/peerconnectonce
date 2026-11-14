import { describe, expect, it } from 'vitest';
import { BoundedPeerWriter, PeerServiceRegistry } from '../src/server/index.js';

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

  it('refuses unbounded application queues without disabling actual buffer backpressure', async () => {
    const fake = channel();
    const writer = new BoundedPeerWriter(fake as never, { maxQueuedBytes: 40, maxMessageBytes: 32, highBytes: 80, lowBytes: 20 });
    expect(await writer.sendText('hello')).toBeUndefined();
    await expect(writer.sendBinary(new Uint8Array(33))).rejects.toThrow('peer_writer_capacity');
    writer.close();
  });
});
