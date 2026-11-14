import { describe, it, expect } from 'vitest';
import { createPeerSignalHandler, MemoryPeerSessionStore } from '../src/signal/index.js';
import { createHttpClientSignaling } from '../src/client/index.js';
import { createHttpNodeSignaling } from '../src/server/index.js';

describe('MemoryPeerSessionStore', () => {
  it('routes one owner/protocol to the chosen node without claim or answer replay', () => {
    let now = 1_000;
    let id = 0;
    const store = new MemoryPeerSessionStore({
      now: () => now, createId: () => 'test' + ++id,
      ttlMs: 500, retentionMs: 500,
    });
    const session = store.create({ nodeId: 'node-a', ownerUserId: 'alice', protocol: 'echo-v1', offerSdp: 'offer' });
    expect(store.read(session, 'bob')).toBeUndefined();
    expect(store.claim('node-b')).toBeUndefined();
    expect(store.claim('node-a')).toEqual({
      sessionId: session, nodeId: 'node-a', ownerUserId: 'alice',
      protocol: 'echo-v1', offerSdp: 'offer',
    });
    expect(store.claim('node-a')).toBeUndefined();
    expect(store.answer('node-b', session, { answerSdp: 'wrong' })).toBe(false);
    expect(store.answer('node-a', session, { answerSdp: 'answer' })).toBe(true);
    expect(store.read(session, 'alice')).toEqual({ status: 'answered', answerSdp: 'answer' });
    expect(store.answer('node-a', session, { answerSdp: 'replay' })).toBe(false);
    now += 501;
    expect(store.read(session, 'alice')).toBeUndefined();
  });

  it('expires unanswered offers, bounds pending ownership and permits cancellation', () => {
    let now = 0;
    let id = 0;
    const store = new MemoryPeerSessionStore({
      ttlMs: 20, retentionMs: 20, maxPendingPerUser: 1,
      now: () => now, createId: () => 'id-' + ++id,
    });
    const row = { nodeId: 'node', ownerUserId: 'u', protocol: 'foo-v1', offerSdp: 'sdp' };
    const a = store.create(row);
    expect(() => store.create(row)).toThrow('peer_capacity');
    now = 21;
    expect(store.read(a, 'u')).toEqual({ status: 'failed' });
    expect(store.claim('node')).toBeUndefined();
    const b = store.create(row);
    store.cancel(b, 'another-user');
    expect(store.read(b, 'u')).toEqual({ status: 'pending' });
    store.cancel(b, 'u');
    expect(store.read(b, 'u')).toEqual({ status: 'failed' });
  });
});

describe('framework-neutral HTTP signaling', () => {
  it('connects authenticated browser and node clients with injection and node isolation', async () => {
    const handler = createPeerSignalHandler({
      authenticateClient: async (request) =>
        request.headers.get('x-user-token') ? { userId: request.headers.get('x-user-token')! } : undefined,
      authenticateNode: async (request) =>
        request.headers.get('authorization') === 'Bearer node-token' ? { nodeId: 'node-a' } : undefined,
      allowedProtocols: ['echo-v1'],
      authorizeService: async ({ userId }) => userId === 'alice',
      getIceServers: async () => [{ urls: 'stun:example.invalid:3478' }],
    });
    const localFetch: typeof fetch = async (input, init) => {
      const request = new Request(input instanceof Request ? input : String(input), init);
      return handler(request);
    };
    const browser = createHttpClientSignaling({
      baseUrl: 'https://broker.example/peer',
      headers: () => ({ 'x-user-token': 'alice' }),
      fetch: localFetch,
    });
    const stranger = createHttpClientSignaling({
      baseUrl: 'https://broker.example/peer',
      headers: () => ({ 'x-user-token': 'bob' }),
      fetch: localFetch,
    });
    const node = createHttpNodeSignaling({
      baseUrl: 'https://broker.example/peer',
      nodeId: 'node-a',
      headers: () => ({ authorization: 'Bearer node-token' }),
      fetch: localFetch,
    });
    expect(await browser.getIceServers({ protocol: 'echo-v1' })).toEqual([{ urls: 'stun:example.invalid:3478' }]);
    const created = await browser.createOffer({ protocol: 'echo-v1', offerSdp: 'test-offer', nodeId: 'node-a' });
    expect(created.sessionId).toBeTruthy();
    // Another owner receives an indistinguishable not-found response.
    await expect(stranger.readAnswer(created.sessionId)).rejects.toThrow('peer_signal_http_404');
    expect(await browser.readAnswer(created.sessionId)).toEqual({ status: 'pending' });
    const claimed = await node.claim();
    expect(claimed).toMatchObject({ nodeId: 'node-a', ownerUserId: 'alice', protocol: 'echo-v1', offerSdp: 'test-offer' });
    expect(await node.claim()).toBeUndefined();
    await node.answer(created.sessionId, 'test-answer');
    expect(await browser.readAnswer(created.sessionId)).toEqual({ status: 'answered', answerSdp: 'test-answer' });
    await expect(node.answer(created.sessionId, 'replay')).rejects.toThrow('peer_answer_http_409');
    await expect(browser.createOffer({ protocol: 'unregistered', offerSdp: 'anything' })).rejects.toThrow('peer_signal_http_403');
    await expect(stranger.createOffer({ protocol: 'echo-v1', offerSdp: 'anything' })).rejects.toThrow('peer_signal_http_403');
  });

  it('cannot leak ICE credentials when the host denies the requested service', async () => {
    let iceCalls = 0;
    const handler = createPeerSignalHandler({
      authenticateClient: async () => ({ userId: 'alice' }),
      authenticateNode: async () => undefined,
      allowedProtocols: ['restricted-v1'],
      authorizeService: async () => false,
      getIceServers: async () => { iceCalls++; return [{ urls: 'turn:private.example:3478', credential: 'private' }]; },
    });
    const url = 'https://broker.example/peer/ice-config?protocol=restricted-v1';
    const result = await handler(new Request(url));
    expect(result.status).toBe(403);
    expect(iceCalls).toBe(0);
  });

  it('provides generic host-specified CORS headers without hardcoding a vendor auth contract', async () => {
    const handler = createPeerSignalHandler({
      authenticateClient: async () => ({ userId: 'user-a' }),
      authenticateNode: async () => undefined,
      allowedProtocols: ['echo-v1'],
      getIceServers: async () => [],
      allowedOrigins: ['https://frontend.example'],
      allowedRequestHeaders: ['x-custom-auth'],
    });
    const response = await handler(new Request('https://broker.example/peer/ice-config?protocol=echo-v1', {
      method: 'OPTIONS',
      headers: { origin: 'https://frontend.example' },
    }));
    expect(response.status).toBe(204);
    expect(response.headers.get('access-control-allow-headers')).toContain('x-custom-auth');
    expect(() => createPeerSignalHandler({
      authenticateClient: async () => undefined,
      authenticateNode: async () => undefined,
      allowedProtocols: ['echo-v1'], getIceServers: async () => [],
      allowedRequestHeaders: ['bad\r\nheader'],
    })).toThrow('invalid_peer_cors_header');
  });

  it('rejects cross-origin requests, unauthorized nodes, and oversized inputs', async () => {
    const handler = createPeerSignalHandler({
      authenticateClient: async () => ({ userId: 'alice' }),
      authenticateNode: async () => undefined,
      allowedProtocols: ['echo-v1'],
      getIceServers: async () => [],
      allowedOrigins: ['https://allowed.example'],
    });
    const root = 'https://broker.example/peer';
    expect((await handler(new Request(root + '/ice-config?protocol=echo-v1',
      { headers: { origin: 'https://evil.example' } }))).status).toBe(403);
    expect((await handler(new Request(root + '/ice-config?protocol=echo-v1',
      { headers: { origin: 'https://allowed.example' } }))).status).toBe(200);
    expect((await handler(new Request(root + '/claim'))).status).toBe(401);
    expect((await handler(new Request(root + '/sessions', {
      method: 'POST', body: JSON.stringify({ protocol: 'echo-v1', offerSdp: 'x'.repeat(300_000) }),
    }))).status).toBe(413);
  });
});
