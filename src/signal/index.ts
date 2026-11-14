import {
  DEFAULT_NODE_ID, isPeerLabel, MAX_SDP_BYTES, readPeerIceServers,
  type PeerOffer, type PeerSessionStatus, type PeerIceServer,
} from '../shared/contracts.js';
export type { PeerOffer, PeerSessionStatus, PeerClientSignaling, PeerNodeSignaling } from '../shared/contracts.js';

/** A terminal signaling result is exclusively a valid answer or a failure. */
export type PeerTerminalResult =
  | { answerSdp: string; failed?: never }
  | { failed: true; answerSdp?: never };

/** Signaling is one-shot rendezvous, not a relay for application data. */
export type PeerSessionStore = {
  create(args: { nodeId: string; ownerUserId: string; protocol: string; offerSdp: string }): Promise<string> | string;
  read(sessionId: string, ownerUserId: string): Promise<PeerSessionStatus | undefined> | PeerSessionStatus | undefined;
  claim(nodeId: string): Promise<PeerOffer | undefined> | PeerOffer | undefined;
  answer(nodeId: string, sessionId: string, result: PeerTerminalResult): Promise<boolean> | boolean;
  /** Receipt acknowledgement, distinct from reading or cancelling a session. */
  acknowledge(sessionId: string, ownerUserId: string): Promise<void> | void;
  cancel(sessionId: string, ownerUserId: string): Promise<void> | void;
};

export type MemoryPeerSessionStoreOptions = {
  /** Pending-offer TTL; independent of an established WebRTC session. */
  ttlMs?: number;
  retentionMs?: number;
  maxSessions?: number;
  maxPendingPerUser?: number;
  /** Bounds pending + retained terminal sessions owned by one user. */
  maxSessionsPerUser?: number;
  now?: () => number;
  createId?: () => string;
};
type StoredSession = PeerOffer & {
  status: PeerSessionStatus;
  claimed: boolean;
  acknowledged: boolean;
  expiresAt: number;
  deleteAt: number;
};

/**
 * Single-process rendezvous store. Live session lookup, per-user capacity and
 * node claims use indexed Maps/Sets, not whole-store scans. Terminal rows are
 * evicted oldest-first only when needed to preserve connection availability.
 */
export class MemoryPeerSessionStore implements PeerSessionStore {
  private readonly sessions = new Map<string, StoredSession>();
  private readonly ownerSessions = new Map<string, Set<string>>();
  private readonly pendingCountByOwner = new Map<string, number>();
  /** Only acknowledged terminal sessions may be reclaimed under capacity pressure. */
  private readonly terminalByOwner = new Map<string, Set<string>>();
  private readonly terminalSessions = new Set<string>();
  private readonly claimableByNode = new Map<string, Set<string>>();
  /** Sweep expiry only when some stored session's deadline has elapsed. */
  private nextSweepAtMs = Infinity;
  private readonly now: () => number;
  private readonly createId: () => string;
  private readonly ttlMs: number;
  private readonly retentionMs: number;
  private readonly maxSessions: number;
  private readonly maxPendingPerUser: number;
  private readonly maxSessionsPerUser: number;

  constructor(options: MemoryPeerSessionStoreOptions = {}) {
    this.now = options.now ?? Date.now;
    this.createId = options.createId ?? (() => crypto.randomUUID());
    this.ttlMs = positiveInteger(options.ttlMs ?? 120_000);
    this.retentionMs = positiveInteger(options.retentionMs ?? 300_000);
    this.maxSessions = positiveInteger(options.maxSessions ?? 128);
    this.maxPendingPerUser = positiveInteger(options.maxPendingPerUser ?? 8);
    this.maxSessionsPerUser = positiveInteger(
      options.maxSessionsPerUser ?? Math.min(this.maxSessions, this.maxPendingPerUser * 2)
    );
  }

  /** Maintain small per-key insertion-ordered indexes in constant time. */
  private addIndex(index: Map<string, Set<string>>, key: string, id: string): void {
    let ids = index.get(key);
    if (!ids) {
      ids = new Set<string>();
      index.set(key, ids);
    }
    ids.add(id);
  }

  /** Remove one indexed id, including the empty owner/node bucket. */
  private removeIndex(index: Map<string, Set<string>>, key: string, id: string): void {
    const ids = index.get(key);
    if (!ids) return;
    ids.delete(id);
    if (ids.size === 0) index.delete(key);
  }

  /** One indexed pending-count transition, without scanning this owner's sessions. */
  private changePendingCount(ownerUserId: string, difference: number): void {
    const next = (this.pendingCountByOwner.get(ownerUserId) ?? 0) + difference;
    if (next === 0) this.pendingCountByOwner.delete(ownerUserId);
    else this.pendingCountByOwner.set(ownerUserId, next);
  }

  /** Delete one session from all indexes without a scan or retained tombstone. */
  private removeSession(session: StoredSession): void {
    this.sessions.delete(session.sessionId);
    this.removeIndex(this.ownerSessions, session.ownerUserId, session.sessionId);
    if (session.status.status === 'pending') {
      this.changePendingCount(session.ownerUserId, -1);
      if (!session.claimed) this.removeIndex(this.claimableByNode, session.nodeId, session.sessionId);
    } else if (session.acknowledged) {
      this.removeIndex(this.terminalByOwner, session.ownerUserId, session.sessionId);
      this.terminalSessions.delete(session.sessionId);
    }
  }

  /** Retain an unacknowledged answer/failure so no other user's offer can steal it. */
  private finishSession(session: StoredSession, status: PeerSessionStatus): void {
    if (!session.claimed) this.removeIndex(this.claimableByNode, session.nodeId, session.sessionId);
    this.changePendingCount(session.ownerUserId, -1);
    session.status = status;
    session.offerSdp = '';
  }

  private expire(): void {
    const now = this.now();
    if (now < this.nextSweepAtMs) return;
    let nextSweepAtMs = Infinity;
    for (const session of this.sessions.values()) {
      if (session.deleteAt <= now) {
        this.removeSession(session);
        continue;
      }
      if (session.status.status === 'pending' && session.expiresAt <= now) {
        this.finishSession(session, { status: 'failed' });
      }
      const deadline = session.status.status === 'pending' ? session.expiresAt : session.deleteAt;
      if (deadline < nextSweepAtMs) nextSweepAtMs = deadline;
    }
    this.nextSweepAtMs = nextSweepAtMs;
  }

  create(args: { nodeId: string; ownerUserId: string; protocol: string; offerSdp: string }): string {
    this.expire();
    const ownerCount = this.ownerSessions.get(args.ownerUserId)?.size ?? 0;
    if ((this.pendingCountByOwner.get(args.ownerUserId) ?? 0) >= this.maxPendingPerUser) {
      throw new Error('peer_capacity');
    }

    // Under pressure, reclaim only a terminal result explicitly acknowledged
    // by its owner. Unread answers, failures and all pending offers are protected.
    // Both safe evictions are O(1) from insertion-ordered Sets.
    if (ownerCount >= this.maxSessionsPerUser) {
      const oldestOwnedTerminal = this.terminalByOwner.get(args.ownerUserId)?.values().next().value;
      if (!oldestOwnedTerminal) throw new Error('peer_capacity');
      this.removeSession(this.sessions.get(oldestOwnedTerminal)!);
    }
    if (this.sessions.size >= this.maxSessions) {
      const oldestTerminal = this.terminalSessions.values().next().value;
      if (!oldestTerminal) throw new Error('peer_capacity');
      this.removeSession(this.sessions.get(oldestTerminal)!);
    }
    let sessionId = this.createId();
    if (this.sessions.has(sessionId)) sessionId = this.createId();
    if (this.sessions.has(sessionId)) throw new Error('peer_capacity');
    const now = this.now();
    this.sessions.set(sessionId, {
      ...args, sessionId, claimed: false, acknowledged: false, status: { status: 'pending' },
      expiresAt: now + this.ttlMs, deleteAt: now + this.ttlMs + this.retentionMs,
    });
    this.addIndex(this.ownerSessions, args.ownerUserId, sessionId);
    this.changePendingCount(args.ownerUserId, 1);
    this.addIndex(this.claimableByNode, args.nodeId, sessionId);
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
    const sessionId = this.claimableByNode.get(nodeId)?.values().next().value;
    if (!sessionId) return undefined;
    const session = this.sessions.get(sessionId)!;
    this.removeIndex(this.claimableByNode, nodeId, sessionId);
    session.claimed = true;
    return {
      sessionId: session.sessionId, nodeId: session.nodeId,
      ownerUserId: session.ownerUserId, protocol: session.protocol, offerSdp: session.offerSdp,
    };
  }

  answer(nodeId: string, sessionId: string, result: PeerTerminalResult): boolean {
    this.expire();
    // Check shape at runtime too: injected stores and JS callers can bypass
    // TypeScript. Never mutate a claimed offer to answered-with-undefined.
    const validFailure = result?.failed === true && result.answerSdp === undefined;
    const validAnswer = result != null && result.failed === undefined &&
      typeof result.answerSdp === 'string' && result.answerSdp.length > 0 &&
      new TextEncoder().encode(result.answerSdp).byteLength <= MAX_SDP_BYTES;
    if (!validFailure && !validAnswer) return false;
    const session = this.sessions.get(sessionId);
    if (!session || session.nodeId !== nodeId || !session.claimed || session.status.status !== 'pending') return false;
    this.finishSession(session, validFailure
      ? { status: 'failed' } : { status: 'answered', answerSdp: result.answerSdp! });
    session.deleteAt = this.now() + this.retentionMs;
    if (session.deleteAt < this.nextSweepAtMs) this.nextSweepAtMs = session.deleteAt;
    return true;
  }

  /** Explicit receipt makes a terminal row evictable, but retains it until pressure/TTL. */
  acknowledge(sessionId: string, ownerUserId: string): void {
    this.expire();
    const session = this.sessions.get(sessionId);
    if (!session || session.ownerUserId !== ownerUserId ||
        session.status.status === 'pending' || session.acknowledged) return;
    session.acknowledged = true;
    this.addIndex(this.terminalByOwner, session.ownerUserId, sessionId);
    this.terminalSessions.add(sessionId);
  }

  cancel(sessionId: string, ownerUserId: string): void {
    this.expire();
    const session = this.sessions.get(sessionId);
    if (!session || session.ownerUserId !== ownerUserId) return;
    // Explicit cancellation frees pending or terminal capacity immediately and
    // fences any in-flight node answer, with no scan or retained tombstone.
    this.removeSession(session);
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
      const acknowledgeMatch = /^sessions\/([a-zA-Z0-9_-]{1,128})\/ack$/.exec(route);
      if (acknowledgeMatch && request.method === 'POST') {
        const user = await browserUser();
        if (!user) return answer({ error: 'unauthorized' }, 401);
        await store.acknowledge(acknowledgeMatch[1]!, user);
        return empty();
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
