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
    expect(store.read(b, 'u')).toBeUndefined();
    expect(store.answer('node', b, { answerSdp: 'too-late' })).toBe(false);
  });

  it('bounds retained answered/failed sessions per owner without blocking another user', () => {
    let serial = 0;
    const store = new MemoryPeerSessionStore({
      ttlMs: 60_000, retentionMs: 60_000,
      maxSessions: 4, maxPendingPerUser: 2, maxSessionsPerUser: 2,
      createId: () => 'terminal-' + ++serial,
    });
    const quiet = store.create({ nodeId: 'other', ownerUserId: 'quiet', protocol: 'echo-v1', offerSdp: 'quiet' });
    let firstNoisy: string | undefined;
    let lastNoisy = '';
    for (let i = 0; i < 40; i++) {
      const id = store.create({ nodeId: 'noisy-node', ownerUserId: 'noisy', protocol: 'echo-v1', offerSdp: String(i) });
      if (!firstNoisy) firstNoisy = id;
      lastNoisy = id;
      expect(store.claim('noisy-node')?.sessionId).toBe(id);
      const isFailure = i % 2 === 0;
      expect(store.answer('noisy-node', id,
        isFailure ? { failed: true } : { answerSdp: 'response-' + i })).toBe(true);
      expect(store.read(id, 'noisy')).toEqual(isFailure
        ? { status: 'failed' } : { status: 'answered', answerSdp: 'response-' + i });
      store.acknowledge(id, 'noisy');
    }
    expect(store.read(firstNoisy!, 'noisy')).toBeUndefined();
    expect(store.read(lastNoisy, 'noisy')).toEqual({ status: 'answered', answerSdp: 'response-39' });
    expect(store.read(quiet, 'quiet')).toEqual({ status: 'pending' });
    expect(store.claim('other')?.sessionId).toBe(quiet);
    const thirdUser = store.create({ nodeId: 'third', ownerUserId: 'third', protocol: 'echo-v1', offerSdp: 'third' });
    expect(store.claim('third')?.sessionId).toBe(thirdUser);
  });

  it('reclaims the oldest terminal globally at capacity without evicting pending offers', () => {
    let serial = 0;
    const store = new MemoryPeerSessionStore({
      maxSessions: 3, maxPendingPerUser: 2, maxSessionsPerUser: 3,
      createId: () => 'global-' + ++serial,
    });
    const row = (ownerUserId: string) =>
      ({ nodeId: 'node', ownerUserId, protocol: 'echo-v1', offerSdp: ownerUserId });
    const pending = store.create(row('pending-owner'));
    const completed = store.create(row('completed-owner'));
    expect(store.claim('node')?.sessionId).toBe(pending);
    expect(store.claim('node')?.sessionId).toBe(completed);
    expect(store.answer('node', completed, { answerSdp: 'done' })).toBe(true);
    store.acknowledge(completed, 'completed-owner');
    const failed = store.create(row('failed-owner'));
    expect(store.claim('node')?.sessionId).toBe(failed);
    expect(store.answer('node', failed, { failed: true })).toBe(true);
    const newcomer = store.create(row('newcomer'));
    expect(store.read(completed, 'completed-owner')).toBeUndefined();
    expect(store.read(failed, 'failed-owner')).toEqual({ status: 'failed' });
    expect(store.read(pending, 'pending-owner')).toEqual({ status: 'pending' });
    expect(store.claim('node')?.sessionId).toBe(newcomer);
    expect(store.answer('node', completed, { answerSdp: 'late' })).toBe(false);
  });

  it('never evicts an answered or failed status that the owner has not acknowledged', () => {
    let serial = 0;
    const store = new MemoryPeerSessionStore({
      ttlMs: 60_000, retentionMs: 60_000,
      maxSessions: 2, maxPendingPerUser: 1, maxSessionsPerUser: 2,
      createId: () => 'protected-' + ++serial,
    });
    const offer = (ownerUserId: string) =>
      ({ nodeId: 'node', ownerUserId, protocol: 'echo-v1', offerSdp: ownerUserId });
    const alice = store.create(offer('alice'));
    expect(store.claim('node')?.sessionId).toBe(alice);
    expect(store.answer('node', alice, { answerSdp: 'alice-answer' })).toBe(true);
    const bob = store.create(offer('bob'));
    expect(store.claim('node')?.sessionId).toBe(bob);
    expect(store.answer('node', bob, { failed: true })).toBe(true);
    // Even a completed body read is not a receipt ACK. Other users must not
    // steal either result when the signaling store is fully occupied.
    expect(store.read(alice, 'alice')).toEqual({ status: 'answered', answerSdp: 'alice-answer' });
    expect(() => store.create(offer('newcomer'))).toThrow('peer_capacity');
    store.acknowledge(bob, 'alice'); // wrong owner cannot acknowledge a result
    expect(() => store.create(offer('newcomer'))).toThrow('peer_capacity');
    expect(store.read(alice, 'alice')).toEqual({ status: 'answered', answerSdp: 'alice-answer' });
    store.acknowledge(bob, 'bob');
    const newcomer = store.create(offer('newcomer'));
    expect(store.read(alice, 'alice')).toEqual({ status: 'answered', answerSdp: 'alice-answer' });
    expect(store.read(bob, 'bob')).toBeUndefined();
    expect(store.claim('node')?.sessionId).toBe(newcomer);
    expect(store.answer('node', bob, { answerSdp: 'late' })).toBe(false);
  });

  it('enforces a per-user limit for unacknowledged terminal results until receipt or expiry', () => {
    let serial = 0;
    const store = new MemoryPeerSessionStore({
      maxSessions: 5, maxPendingPerUser: 2, maxSessionsPerUser: 2,
      createId: () => 'owner-' + ++serial,
    });
    const input = { nodeId: 'node', ownerUserId: 'busy', protocol: 'echo-v1', offerSdp: 'x' };
    const a = store.create(input);
    expect(store.claim('node')?.sessionId).toBe(a);
    store.answer('node', a, { answerSdp: 'result' });
    const b = store.create(input);
    expect(store.claim('node')?.sessionId).toBe(b);
    store.answer('node', b, { failed: true });
    expect(() => store.create(input)).toThrow('peer_capacity');
    store.acknowledge(a, 'busy');
    const c = store.create(input);
    expect(store.read(a, 'busy')).toBeUndefined();
    expect(store.read(b, 'busy')).toEqual({ status: 'failed' });
    expect(store.claim('node')?.sessionId).toBe(c);
  });

  it('expires an unacknowledged terminal result without requiring an acknowledgement', () => {
    let now = 1_000;
    let serial = 0;
    const store = new MemoryPeerSessionStore({
      ttlMs: 10, retentionMs: 20,
      maxSessions: 1, maxSessionsPerUser: 1, maxPendingPerUser: 1,
      now: () => now,
      createId: () => 'expire-' + ++serial,
    });
    const first = store.create({ nodeId: 'node', ownerUserId: 'alice', protocol: 'echo-v1', offerSdp: 'first' });
    expect(store.claim('node')?.sessionId).toBe(first);
    expect(store.answer('node', first, { answerSdp: 'answer' })).toBe(true);
    expect(() => store.create({
      nodeId: 'node', ownerUserId: 'bob', protocol: 'echo-v1', offerSdp: 'second',
    })).toThrow('peer_capacity');
    now += 21;
    expect(store.read(first, 'alice')).toBeUndefined();
    const second = store.create({ nodeId: 'node', ownerUserId: 'bob', protocol: 'echo-v1', offerSdp: 'second' });
    expect(store.claim('node')?.sessionId).toBe(second);
  });

  it('rejects capacity overflow when all sessions are pending rather than silently evicting work', () => {
    let serial = 0;
    const store = new MemoryPeerSessionStore({
      maxSessions: 2, maxPendingPerUser: 2, maxSessionsPerUser: 2,
      createId: () => 'pending-' + ++serial,
    });
    const input = { nodeId: 'node', ownerUserId: 'alice', protocol: 'echo-v1', offerSdp: 'x' };
    const a = store.create(input);
    const b = store.create(input);
    expect(() => store.create({ ...input, ownerUserId: 'bob' })).toThrow('peer_capacity');
    expect(store.claim('node')?.sessionId).toBe(a);
    expect(store.claim('node')?.sessionId).toBe(b);
    expect(store.claim('node')).toBeUndefined();
    store.cancel(a, 'alice');
    const c = store.create({ ...input, ownerUserId: 'bob' });
    expect(store.claim('node')?.sessionId).toBe(c);
  });

  it('allows repeated cancellation without exhausting global retained-session capacity', () => {
    let counter = 0;
    const store = new MemoryPeerSessionStore({
      createId: () => 'cancel-' + ++counter,
      maxSessions: 2, maxPendingPerUser: 1,
      ttlMs: 60_000, retentionMs: 60_000,
    });
    for (let index = 0; index < 20; index++) {
      const id = store.create({ nodeId: 'node', ownerUserId: 'u', protocol: 'read-v1', offerSdp: 'offer' });
      store.cancel(id, 'u');
      expect(store.read(id, 'u')).toBeUndefined();
      expect(store.claim('node')).toBeUndefined();
    }
    expect(counter).toBe(20);
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

  it('protects a polled answer through HTTP capacity pressure until explicit client acknowledgement', async () => {
    let serial = 0;
    const handler = createPeerSignalHandler({
      allowedProtocols: ['echo-v1'],
      getIceServers: async () => [],
      store: new MemoryPeerSessionStore({
        maxSessions: 1, maxPendingPerUser: 1, maxSessionsPerUser: 1,
        createId: () => 'http-' + ++serial,
      }),
      authenticateClient: async (request) => {
        const id = request.headers.get('x-test-user');
        return id ? { userId: id } : undefined;
      },
      authenticateNode: async () => ({ nodeId: 'node' }),
    });
    const fetchLocal: typeof fetch = async (url, options) =>
      handler(new Request(String(url), options));
    const client = (user: string) => createHttpClientSignaling({
      baseUrl: 'https://broker.example/peer',
      headers: () => ({ 'x-test-user': user }),
      fetch: fetchLocal,
    });
    const alice = client('alice');
    const bob = client('bob');
    const session = await alice.createOffer({ protocol: 'echo-v1', offerSdp: 'offer', nodeId: 'node' });
    const node = createHttpNodeSignaling({
      baseUrl: 'https://broker.example/peer', nodeId: 'node',
      headers: () => ({}), fetch: fetchLocal,
    });
    expect((await node.claim())?.sessionId).toBe(session.sessionId);
    await node.answer(session.sessionId, 'answer');
    expect(await alice.readAnswer(session.sessionId)).toEqual({ status: 'answered', answerSdp: 'answer' });
    await expect(bob.createOffer({ protocol: 'echo-v1', offerSdp: 'bob-offer' })).rejects.toThrow('peer_signal_http_429');
    await alice.acknowledgeOffer?.(session.sessionId);
    const bobSession = await bob.createOffer({ protocol: 'echo-v1', offerSdp: 'bob-offer', nodeId: 'node' });
    await expect(alice.readAnswer(session.sessionId)).rejects.toThrow('peer_signal_http_404');
    expect((await node.claim())?.sessionId).toBe(bobSession.sessionId);
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
