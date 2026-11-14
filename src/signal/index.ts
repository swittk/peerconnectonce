import {
  DEFAULT_NODE_ID, isPeerLabel, MAX_SDP_BYTES, readPeerIceServers,
  type PeerOffer, type PeerSessionStatus, type PeerIceServer,
} from '../shared/contracts.js';
export type { PeerOffer, PeerSessionStatus, PeerClientSignaling, PeerNodeSignaling } from '../shared/contracts.js';

/** Signaling is one-shot rendezvous, not a relay for application data. */
export type PeerSessionStore = {
  create(args: { nodeId: string; ownerUserId: string; protocol: string; offerSdp: string }): Promise<string> | string;
  read(sessionId: string, ownerUserId: string): Promise<PeerSessionStatus | undefined> | PeerSessionStatus | undefined;
  claim(nodeId: string): Promise<PeerOffer | undefined> | PeerOffer | undefined;
  answer(nodeId: string, sessionId: string, result: { answerSdp?: string; failed?: true }): Promise<boolean> | boolean;
  cancel(sessionId: string, ownerUserId: string): Promise<void> | void;
};

export type MemoryPeerSessionStoreOptions = {
  /** Pending-offer TTL; independent of an established WebRTC session. */
  ttlMs?: number;
  retentionMs?: number;
  maxSessions?: number;
  maxPendingPerUser?: number;
  now?: () => number;
  createId?: () => string;
};
type StoredSession = PeerOffer & {
  status: PeerSessionStatus;
  claimed: boolean;
  expiresAt: number;
  deleteAt: number;
};

/** Single-process store; applications requiring HA inject their own store instead. */
export class MemoryPeerSessionStore implements PeerSessionStore {
  private readonly sessions = new Map<string, StoredSession>();
  /** Skip full-store expiry sweeps until an actual session deadline is due. */
  private nextSweepAtMs = Infinity;
  private readonly now: () => number;
  private readonly createId: () => string;
  private readonly ttlMs: number;
  private readonly retentionMs: number;
  private readonly maxSessions: number;
  private readonly maxPendingPerUser: number;

  constructor(options: MemoryPeerSessionStoreOptions = {}) {
    this.now = options.now ?? Date.now;
    this.createId = options.createId ?? (() => crypto.randomUUID());
    this.ttlMs = positiveInteger(options.ttlMs ?? 120_000);
    this.retentionMs = positiveInteger(options.retentionMs ?? 300_000);
    this.maxSessions = positiveInteger(options.maxSessions ?? 128);
    this.maxPendingPerUser = positiveInteger(options.maxPendingPerUser ?? 8);
  }

  private expire(): void {
    const now = this.now();
    if (now < this.nextSweepAtMs) return;
    let nextSweepAtMs = Infinity;
    for (const [id, session] of this.sessions) {
      if (session.deleteAt <= now) {
        this.sessions.delete(id);
        continue;
      }
      if (session.status.status === 'pending' && session.expiresAt <= now) {
        session.offerSdp = '';
        session.status = { status: 'failed' };
      }
      const deadline = session.status.status === 'pending' ? session.expiresAt : session.deleteAt;
      if (deadline < nextSweepAtMs) nextSweepAtMs = deadline;
    }
    this.nextSweepAtMs = nextSweepAtMs;
  }

  create(args: { nodeId: string; ownerUserId: string; protocol: string; offerSdp: string }): string {
    this.expire();
    if (this.sessions.size >= this.maxSessions) throw new Error('peer_capacity');
    let count = 0;
    for (const session of this.sessions.values()) {
      if (session.ownerUserId === args.ownerUserId && session.status.status === 'pending') count++;
    }
    if (count >= this.maxPendingPerUser) throw new Error('peer_capacity');
    let sessionId = this.createId();
    if (this.sessions.has(sessionId)) sessionId = this.createId();
    if (this.sessions.has(sessionId)) throw new Error('peer_capacity');
    const now = this.now();
    this.sessions.set(sessionId, {
      ...args, sessionId, claimed: false, status: { status: 'pending' },
      expiresAt: now + this.ttlMs, deleteAt: now + this.ttlMs + this.retentionMs,
    });
    if (now + this.ttlMs < this.nextSweepAtMs) this.nextSweepAtMs = now + this.ttlMs;
    return sessionId;
  }

  read(sessionId: string, ownerUserId: string): PeerSessionStatus | undefined {
    this.expire();
    const session = this.sessions.get(sessionId);
    if (!session || session.ownerUserId !== ownerUserId) return undefined;
    return session.status;
  }

  claim(nodeId: string): PeerOffer | undefined {
    this.expire();
    for (const session of this.sessions.values()) {
      if (session.nodeId !== nodeId || session.claimed || session.status.status !== 'pending') continue;
      session.claimed = true;
      return {
        sessionId: session.sessionId, nodeId: session.nodeId,
        ownerUserId: session.ownerUserId, protocol: session.protocol, offerSdp: session.offerSdp,
      };
    }
    return undefined;
  }

  answer(nodeId: string, sessionId: string, result: { answerSdp?: string; failed?: true }): boolean {
    this.expire();
    const session = this.sessions.get(sessionId);
    if (!session || session.nodeId !== nodeId || !session.claimed || session.status.status !== 'pending') return false;
    session.status = result.failed ? { status: 'failed' } : { status: 'answered', answerSdp: result.answerSdp! };
    session.offerSdp = '';
    session.deleteAt = this.now() + this.retentionMs;
    if (session.deleteAt < this.nextSweepAtMs) this.nextSweepAtMs = session.deleteAt;
    return true;
  }

  cancel(sessionId: string, ownerUserId: string): void {
    this.expire();
    const session = this.sessions.get(sessionId);
    if (!session || session.ownerUserId !== ownerUserId) return;
    // Explicit cancellation needs no retained SDP/status row. Immediate deletion
    // frees capacity and fences an in-flight node answer without another scan.
    this.sessions.delete(sessionId);
  }
}

function positiveInteger(n: number): number {
  if (!Number.isSafeInteger(n) || n <= 0) throw new Error('invalid_peer_limit');
  return n;
}

export type SignalIdentity = { userId: string };
export type SignalNodeIdentity = { nodeId: string };
export type PeerSignalHandlerOptions = {
  /** The host supplies its own session and role authentication. */
  authenticateClient: (request: Request) => Promise<SignalIdentity | undefined>;
  /** Each node is authenticated separately, never from browser-submitted node IDs. */
  authenticateNode: (request: Request) => Promise<SignalNodeIdentity | undefined>;
  allowedProtocols: readonly string[];
  /** Optional host-specific patient/service grant; evaluated before storing an offer. */
  authorizeService?: (args: { userId: string; nodeId: string; protocol: string }) => Promise<boolean>;
  resolveNode?: (args: { userId: string; protocol: string; requestedNodeId?: string }) => Promise<string | undefined>;
  getIceServers: (nodeId: string) => Promise<PeerIceServer[]>;
  store?: PeerSessionStore;
  basePath?: string;
  allowedOrigins?: readonly string[];
  /** Additional host-specific request headers allowed on browser CORS preflight. */
  allowedRequestHeaders?: readonly string[];
};

function json(value: object, status = 200): Response {
  return new Response(JSON.stringify(value), { status, headers: { 'content-type': 'application/json' } });
}
function record(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

/** Bound untrusted JSON before parsing; the offer maximum is 256KiB. */
async function readJsonBody(request: Request): Promise<unknown> {
  const length = Number(request.headers.get('content-length') || 0);
  if (length > MAX_SDP_BYTES + 16_384) throw new Error('body_too_large');
  const reader = request.body?.getReader();
  if (!reader) return undefined;
  let received = 0;
  const chunks: Uint8Array[] = [];
  try {
    while (true) {
      const chunk = await reader.read();
      if (chunk.done) break;
      received += chunk.value.byteLength;
      if (received > MAX_SDP_BYTES + 16_384) throw new Error('body_too_large');
      chunks.push(chunk.value);
    }
  } finally {
    reader.releaseLock();
  }
  const bytes = new Uint8Array(received);
  let offset = 0;
  for (const chunk of chunks) { bytes.set(chunk, offset); offset += chunk.byteLength; }
  try { return JSON.parse(new TextDecoder().decode(bytes)); } catch { throw new Error('invalid_json'); }
}

/** Framework-neutral HTTP handler: mount it in Express, Fastify, Hono, Node, or another host. */
export function createPeerSignalHandler(options: PeerSignalHandlerOptions): (request: Request) => Promise<Response> {
  const basePath = (options.basePath ?? '/peer').replace(/\/$/, '');
  if (!basePath.startsWith('/') || basePath.startsWith('//')) throw new Error('invalid_peer_base_path');
  const protocols = new Set(options.allowedProtocols);
  if (!protocols.size || protocols.size > 32) throw new Error('invalid_peer_service_registration');
  for (const label of protocols) if (!isPeerLabel(label)) throw new Error('invalid_peer_service_registration');
  const store = options.store ?? new MemoryPeerSessionStore();
  const allowedHeaders = new Set(['content-type', 'authorization']);
  for (const header of options.allowedRequestHeaders ?? []) {
    if (!/^[a-z0-9-]+$/i.test(header)) throw new Error('invalid_peer_cors_header');
    allowedHeaders.add(header.toLowerCase());
  }
  const corsAllowedHeaders = [...allowedHeaders].join(', ');

  return async (request: Request): Promise<Response> => {
    const url = new URL(request.url);
    const path = url.pathname;
    const prefix = basePath + '/';
    if (!path.startsWith(prefix)) return json({ error: 'not_found' }, 404);
    const route = path.slice(prefix.length);
    const origin = request.headers.get('origin');
    const headers = new Headers({ 'cache-control': 'private, no-store' });
    if (origin) {
      if (!options.allowedOrigins?.includes(origin) && origin !== url.origin) return json({ error: 'forbidden_origin' }, 403);
      headers.set('access-control-allow-origin', origin);
      headers.set('vary', 'origin');
      headers.set('access-control-allow-methods', 'GET, POST, DELETE, OPTIONS');
      headers.set('access-control-allow-headers', corsAllowedHeaders);
      headers.set('access-control-max-age', '600');
    }
    if (request.method === 'OPTIONS') return new Response(null, { status: 204, headers });

    async function browserUser(): Promise<string | undefined> {
      return (await options.authenticateClient(request))?.userId;
    }
    async function nodeIdentity(): Promise<string | undefined> {
      return (await options.authenticateNode(request))?.nodeId;
    }
    async function resolvedNode(userId: string, protocol: string, requestedNodeId?: string): Promise<string | undefined> {
      return options.resolveNode
        ? options.resolveNode({ userId, protocol, requestedNodeId })
        : requestedNodeId ?? DEFAULT_NODE_ID;
    }
    function answer(value: object, status = 200): Response {
      const response = json(value, status);
      headers.forEach((val, name) => response.headers.set(name, val));
      return response;
    }
    function empty(status = 204): Response { return new Response(null, { status, headers }); }

    try {
      if (request.method === 'GET' && route === 'ice-config') {
        const user = await browserUser();
        if (!user) return answer({ error: 'unauthorized' }, 401);
        const protocol = url.searchParams.get('protocol') ?? '';
        if (!protocols.has(protocol)) return answer({ error: 'unregistered_protocol' }, 403);
        const node = await resolvedNode(user, protocol, url.searchParams.get('nodeId') ?? undefined);
        if (!node || node.length > 128) return answer({ error: 'node_unavailable' }, 404);
        if (options.authorizeService && !await options.authorizeService({ userId: user, nodeId: node, protocol }))
          return answer({ error: 'forbidden_protocol' }, 403);
        return answer({ iceServers: readPeerIceServers(await options.getIceServers(node)) });
      }
      if (request.method === 'POST' && route === 'sessions') {
        const user = await browserUser();
        if (!user) return answer({ error: 'unauthorized' }, 401);
        const body = await readJsonBody(request);
        if (!record(body) || typeof body.protocol !== 'string' || !protocols.has(body.protocol))
          return answer({ error: 'unregistered_protocol' }, 403);
        if (typeof body.offerSdp !== 'string' || body.offerSdp.length === 0 ||
            new TextEncoder().encode(body.offerSdp).length > MAX_SDP_BYTES)
          return answer({ error: 'invalid_offer' }, 400);
        if (body.nodeId !== undefined && (typeof body.nodeId !== 'string' || body.nodeId.length > 128))
          return answer({ error: 'invalid_node' }, 400);
        const node = await resolvedNode(user, body.protocol, body.nodeId as string | undefined);
        if (!node || node.length > 128) return answer({ error: 'node_unavailable' }, 404);
        if (options.authorizeService && !await options.authorizeService({ userId: user, nodeId: node, protocol: body.protocol }))
          return answer({ error: 'forbidden_protocol' }, 403);
        const iceServers = readPeerIceServers(await options.getIceServers(node));
        const sessionId = await store.create({ nodeId: node, ownerUserId: user, protocol: body.protocol, offerSdp: body.offerSdp });
        return answer({ sessionId, iceServers }, 201);
      }
      const sessionMatch = /^sessions\/([a-zA-Z0-9_-]{1,128})$/.exec(route);
      if (sessionMatch && (request.method === 'GET' || request.method === 'DELETE')) {
        const user = await browserUser();
        if (!user) return answer({ error: 'unauthorized' }, 401);
        const id = sessionMatch[1]!;
        if (request.method === 'DELETE') { await store.cancel(id, user); return empty(); }
        const status = await store.read(id, user);
        return status ? answer(status) : answer({ error: 'not_found' }, 404);
      }
      if (request.method === 'GET' && route === 'claim') {
        const node = await nodeIdentity();
        if (!node) return answer({ error: 'unauthorized' }, 401);
        const offer = await store.claim(node);
        return offer ? answer(offer) : empty();
      }
      if (request.method === 'POST' && route === 'answer') {
        const node = await nodeIdentity();
        if (!node) return answer({ error: 'unauthorized' }, 401);
        const body = await readJsonBody(request);
        if (!record(body) || typeof body.sessionId !== 'string' ||
            body.sessionId.length === 0 || body.sessionId.length > 128 ||
            (typeof body.answerSdp === 'string') === (body.failed === true) ||
            (typeof body.answerSdp === 'string' && body.answerSdp.length === 0))
          return answer({ error: 'invalid_request' }, 400);
        if (typeof body.answerSdp === 'string' &&
            new TextEncoder().encode(body.answerSdp).byteLength > MAX_SDP_BYTES)
          return answer({ error: 'answer_too_large' }, 413);
        const ok = await store.answer(node, body.sessionId, body.failed === true
          ? { failed: true } : { answerSdp: body.answerSdp as string });
        return ok ? empty() : answer({ error: 'not_claimable' }, 409);
      }
      return answer({ error: 'not_found' }, 404);
    } catch (error) {
      if (error instanceof Error) {
        if (error.message === 'peer_capacity') return answer({ error: 'peer_capacity' }, 429);
        if (error.message === 'body_too_large') return answer({ error: 'body_too_large' }, 413);
        if (error.message === 'invalid_json') return answer({ error: 'invalid_json' }, 400);
      }
      return answer({ error: 'signaling_unavailable' }, 503);
    }
  };
}
