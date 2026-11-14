import {
  isPeerLabel, readPeerIceServers,
  type PeerClientSignaling, type PeerSessionCreation, type PeerSessionStatus, type PeerIceServer,
  type PeerHttpHeaders,
} from '../shared/contracts.js';

export type { PeerClientSignaling, PeerSessionCreation, PeerSessionStatus } from '../shared/contracts.js';

/** A transport-neutral WebRTC-compatible interface; no native browser objects. */
export type PeerConnectionConfiguration = { iceServers: PeerIceServer[] };
export interface ClientPeerChannel {
  binaryType: string;
  readonly readyState: 'connecting' | 'open' | 'closing' | 'closed';
  addEventListener(name: 'open' | 'close' | 'error' | 'bufferedamountlow', handler: () => void, options?: { once?: boolean }): void;
  addEventListener(name: 'message', handler: (event: { data: unknown }) => void, options?: { once?: boolean }): void;
  removeEventListener(name: 'open' | 'close' | 'error' | 'bufferedamountlow', handler: () => void): void;
  removeEventListener(name: 'message', handler: (event: { data: unknown }) => void): void;
  send(message: string | ArrayBuffer | Uint8Array): void;
  close(): void;
}
export interface ClientPeerConnection {
  readonly iceGatheringState: 'new' | 'gathering' | 'complete';
  readonly connectionState: 'new' | 'connecting' | 'connected' | 'disconnected' | 'failed' | 'closed';
  readonly localDescription: { sdp?: string } | null;
  addEventListener(name: 'icegatheringstatechange' | 'connectionstatechange', handler: () => void, options?: { once?: boolean }): void;
  removeEventListener(name: 'icegatheringstatechange' | 'connectionstatechange', handler: () => void): void;
  createDataChannel(label: string, options: { ordered: true }): ClientPeerChannel;
  createOffer(): Promise<{ type: 'offer'; sdp?: string }>;
  setLocalDescription(description: { type: 'offer'; sdp?: string }): Promise<void>;
  setRemoteDescription(description: { type: 'answer'; sdp?: string }): Promise<void>;
  close(): void;
}

export type PeerConnectionFactory = (config: PeerConnectionConfiguration) => ClientPeerConnection;
export type PeerClientOptions = {
  signaling: PeerClientSignaling;
  /** Native WebRTC is injected by a platform adapter such as /browser. */
  createPeerConnection: PeerConnectionFactory;
  connectionTimeoutMs?: number;
  iceGatheringTimeoutMs?: number;
  answerPollIntervalMs?: number;
};
export type PeerClientConnectionHandle = {
  /** A reliable ordered data channel; app adapters own their message format. */
  channel: ClientPeerChannel;
  readonly protocol: string;
  readonly signal: AbortSignal;
  close(): void;
};

function validMilliseconds(value: number): number {
  if (!Number.isFinite(value) || value <= 0 || value > 3_600_000) throw new Error('invalid_peer_timeout');
  return value;
}

/** A deadline shared by all non-trickle SDP operations, with real abort cleanup. */
function underDeadline<T>(
  promise: Promise<T>,
  deadline: number,
  signal: AbortSignal,
  operation: string,
): Promise<T> {
  const remaining = deadline - Date.now();
  if (remaining <= 0) return Promise.reject(new Error('peer_timeout:' + operation));
  if (signal.aborted) return Promise.reject(new Error('peer_aborted'));
  return new Promise<T>((resolve, reject) => {
    let settled = false;
    let timer: ReturnType<typeof setTimeout>;
    const cleanup = () => {
      clearTimeout(timer);
      signal.removeEventListener('abort', onAbort);
    };
    const finish = (err: Error | undefined, result?: T) => {
      if (settled) return;
      settled = true;
      cleanup();
      if (err) reject(err); else resolve(result as T);
    };
    const onAbort = () => finish(new Error('peer_aborted'));
    signal.addEventListener('abort', onAbort, { once: true });
    timer = setTimeout(() => finish(new Error('peer_timeout:' + operation)), remaining);
    promise.then((result) => finish(undefined, result), (reason: unknown) =>
      finish(reason instanceof Error ? reason : new Error('peer_' + operation + '_failed')));
  });
}

/** Negotiate full candidates without trickling; handle a synchronous ICE completion. */
async function gatherIce(connection: ClientPeerConnection, deadline: number, signal: AbortSignal): Promise<void> {
  if (connection.iceGatheringState === 'complete') return;
  let unsubscribe: (() => void) | undefined;
  try {
    await underDeadline(new Promise<void>((resolve) => {
      const listener = () => {
        if (connection.iceGatheringState === 'complete') resolve();
      };
      connection.addEventListener('icegatheringstatechange', listener);
      unsubscribe = () => connection.removeEventListener('icegatheringstatechange', listener);
      listener();
    }), deadline, signal, 'ice_gather');
  } finally {
    unsubscribe?.();
  }
}

/** Open only after the browser's SCTP channel is ready; never return a half-open channel. */
async function waitForChannel(connection: ClientPeerConnection, channel: ClientPeerChannel, deadline: number, signal: AbortSignal): Promise<void> {
  if (channel.readyState === 'open') return;
  let unsubscribe: (() => void) | undefined;
  try {
    await underDeadline(new Promise<void>((resolve, reject) => {
      const onOpen = () => resolve();
      const onClosed = () => reject(new Error('peer_channel_failed'));
      const onConnection = () => {
        if (connection.connectionState === 'failed' || connection.connectionState === 'closed') onClosed();
      };
      channel.addEventListener('open', onOpen, { once: true });
      channel.addEventListener('close', onClosed, { once: true });
      channel.addEventListener('error', onClosed, { once: true });
      connection.addEventListener('connectionstatechange', onConnection);
      unsubscribe = () => {
        channel.removeEventListener('open', onOpen);
        channel.removeEventListener('close', onClosed);
        channel.removeEventListener('error', onClosed);
        connection.removeEventListener('connectionstatechange', onConnection);
      };
      if (channel.readyState === 'open') onOpen();
    }), deadline, signal, 'channel_open');
  } finally {
    unsubscribe?.();
  }
}

function sleep(milliseconds: number, signal: AbortSignal): Promise<void> {
  if (signal.aborted) return Promise.reject(new Error('peer_aborted'));
  return new Promise((resolve, reject) => {
    const cleanup = () => signal.removeEventListener('abort', cancel);
    const cancel = () => {
      clearTimeout(timer);
      cleanup();
      reject(new Error('peer_aborted'));
    };
    const timer = setTimeout(() => { cleanup(); resolve(); }, milliseconds);
    signal.addEventListener('abort', cancel, { once: true });
  });
}

/** Compare one validated ICE configuration without serializing it or inventing a STUN fallback. */
function sameIceServers(left: PeerIceServer[], right: PeerIceServer[]): boolean {
  if (left.length !== right.length) return false;
  for (let index = 0; index < left.length; index += 1) {
    const a = left[index]!;
    const b = right[index]!;
    if (a.username !== b.username || a.credential !== b.credential) return false;
    const leftUrls = typeof a.urls === 'string' ? [a.urls] : a.urls;
    const rightUrls = typeof b.urls === 'string' ? [b.urls] : b.urls;
    if (leftUrls.length !== rightUrls.length) return false;
    for (let part = 0; part < leftUrls.length; part += 1) {
      if (leftUrls[part] !== rightUrls[part]) return false;
    }
  }
  return true;
}

/** Shared platform-neutral signaling/ICE/SDP/reconnect implementation for every service adapter. */
export class PeerClient {
  private readonly signaling: PeerClientSignaling;
  private readonly makeConnection: PeerConnectionFactory;
  private readonly connectionTimeoutMs: number;
  private readonly iceGatheringTimeoutMs: number;
  private readonly answerPollIntervalMs: number;
  private active?: PeerClientConnectionHandle;
  private activeNodeId?: string;
  private pending?: Promise<PeerClientConnectionHandle>;
  private pendingProtocol?: string;
  private pendingNodeId?: string;
  private connectionAbort?: AbortController;
  private permanentlyClosed = false;

  constructor(options: PeerClientOptions) {
    this.signaling = options.signaling;
    this.makeConnection = options.createPeerConnection;
    this.connectionTimeoutMs = validMilliseconds(options.connectionTimeoutMs ?? 30_000);
    this.iceGatheringTimeoutMs = validMilliseconds(options.iceGatheringTimeoutMs ?? 12_000);
    this.answerPollIntervalMs = validMilliseconds(options.answerPollIntervalMs ?? 500);
  }

  /** Reuse one live connection or open a fresh one after disconnect. */
  async connect(protocol: string, options: { nodeId?: string; signal?: AbortSignal } = {}): Promise<PeerClientConnectionHandle> {
    if (this.permanentlyClosed) throw new Error('peer_client_closed');
    if (options.signal?.aborted) throw new Error('peer_aborted');
    if (!isPeerLabel(protocol)) throw new Error('invalid_peer_protocol');
    const requestedNodeId = options.nodeId;
    if (this.active?.channel.readyState === 'open') {
      if (this.active.protocol !== protocol || this.activeNodeId !== requestedNodeId)
        throw new Error('peer_client_in_use');
      return this.active;
    }
    if (this.pending) {
      if (this.pendingProtocol !== protocol || this.pendingNodeId !== requestedNodeId)
        throw new Error('peer_client_in_use');
      return this.pending;
    }
    this.connectionAbort?.abort();
    const controller = new AbortController();
    this.connectionAbort = controller;
    const forwardAbort = () => controller.abort();
    options.signal?.addEventListener('abort', forwardAbort, { once: true });
    this.pendingProtocol = protocol;
    this.pendingNodeId = requestedNodeId;
    const work = this.open(protocol, options.nodeId, controller).finally(() => {
      options.signal?.removeEventListener('abort', forwardAbort);
      if (this.pending === work) {
        this.pending = undefined;
        this.pendingProtocol = undefined;
        this.pendingNodeId = undefined;
      }
    });
    this.pending = work;
    return work;
  }

  /** Tear down all requests belonging to this client; a new instance can be created later. */
  close(): void {
    if (this.permanentlyClosed) return;
    this.permanentlyClosed = true;
    const active = this.active;
    this.active = undefined;
    this.activeNodeId = undefined;
    active?.close();
    this.connectionAbort?.abort();
  }

  private async open(
    protocol: string, nodeId: string | undefined, controller: AbortController,
    replacementIce?: PeerIceServer[], deadline = Date.now() + this.connectionTimeoutMs,
    canRetryLateIce = true,
  ): Promise<PeerClientConnectionHandle> {
    const signal = controller.signal;
    let peer: ClientPeerConnection | undefined;
    let channel: ClientPeerChannel | undefined;
    let sessionId: string | undefined;
    let alreadyClosed = false;
    let attemptClosed = false;
    const closeAttempt = () => {
      if (attemptClosed) return;
      attemptClosed = true;
      try { channel?.close(); } catch { /* already closed */ }
      try { peer?.close(); } catch { /* already closed */ }
    };
    const close = () => {
      if (alreadyClosed) return;
      alreadyClosed = true;
      controller.abort();
      closeAttempt();
    };
    try {
      const ice = replacementIce ?? readPeerIceServers(await underDeadline(
        this.signaling.getIceServers({ protocol, nodeId, signal }), deadline, signal, 'ice_config'
      ));
      peer = this.makeConnection({ iceServers: ice });
      channel = peer.createDataChannel(protocol, { ordered: true });
      channel.binaryType = 'arraybuffer';
      const offer = await underDeadline(peer.createOffer(), deadline, signal, 'create_offer');
      await underDeadline(peer.setLocalDescription(offer), deadline, signal, 'local_description');
      await gatherIce(peer, Math.min(deadline, Date.now() + this.iceGatheringTimeoutMs), signal);
      const offerSdp = peer.localDescription?.sdp;
      if (!offerSdp) throw new Error('peer_missing_local_sdp');
      const session = await underDeadline(
        this.signaling.createOffer({ protocol, offerSdp, ...(nodeId ? { nodeId } : {}), signal }),
        deadline, signal, 'signal_offer'
      );
      sessionId = session.sessionId;
      // Older signal hosts may not offer /ice-config. If the session reply supplies
      // TURN after a host-only offer, that SDP cannot be retrofitted: make one fresh offer.
      const sessionIce = readPeerIceServers(session.iceServers);
      if (canRetryLateIce && sessionIce.length > 0 && !sameIceServers(ice, sessionIce)) {
        // This one-shot cancellation is already accounted for below; a failure
        // must not trigger a second unbounded request in the outer catch.
        const supersededSessionId = sessionId;
        sessionId = undefined;
        // Bound this request by the same connection deadline. Its own AbortController
        // stops a stalled HTTP DELETE without prematurely terminating the new offer.
        if (this.signaling.cancelOffer) {
          const cancelController = new AbortController();
          const cancelForParent = () => cancelController.abort();
          signal.addEventListener('abort', cancelForParent, { once: true });
          try {
            await underDeadline(
              this.signaling.cancelOffer(supersededSessionId, cancelController.signal),
              deadline, signal, 'late_ice_cancel'
            );
          } catch {
            cancelController.abort();
            // An orphaned offer still expires under the signaling store's TTL.
          } finally {
            signal.removeEventListener('abort', cancelForParent);
          }
        }
        if (Date.now() >= deadline) throw new Error('peer_timeout:late_ice_cancel');
        closeAttempt();
        return this.open(protocol, nodeId, controller, sessionIce, deadline, false);
      }
      let answer: string | undefined;
      while (!answer) {
        const state = await underDeadline(this.signaling.readAnswer(sessionId, signal), deadline, signal, 'signal_answer');
        if (state.status === 'failed') throw new Error('peer_offer_rejected');
        if (state.status === 'answered') {
          if (typeof state.answerSdp !== 'string' || !state.answerSdp) {
            throw new Error('invalid_peer_answer');
          }
          answer = state.answerSdp;
        } else if (state.status === 'pending') {
          await underDeadline(sleep(this.answerPollIntervalMs, signal), deadline, signal, 'answer_poll');
        } else {
          throw new Error('invalid_peer_answer');
        }
      }
      await underDeadline(peer.setRemoteDescription({ type: 'answer', sdp: answer }), deadline, signal, 'remote_description');
      await waitForChannel(peer, channel, deadline, signal);
      // Only the browser that successfully received/applied the answer can
      // acknowledge it. Until this arrives, the signaling store protects
      // the answer from eviction by another user's connection attempt.
      if (this.signaling.acknowledgeOffer) {
        const acknowledgeController = new AbortController();
        const acknowledgeTimer = setTimeout(() => acknowledgeController.abort(), 5_000);
        void Promise.resolve()
          .then(() => this.signaling.acknowledgeOffer!(sessionId!, acknowledgeController.signal))
          .catch(() => undefined)
          .finally(() => clearTimeout(acknowledgeTimer));
      }
      const handlePeerState = () => {
        if (peer?.connectionState === 'failed' || peer?.connectionState === 'closed') close();
      };
      peer.addEventListener('connectionstatechange', handlePeerState);
      channel.addEventListener('close', close, { once: true });
      const result: PeerClientConnectionHandle = { protocol, channel, signal, close };
      this.active = result;
      this.activeNodeId = nodeId;
      signal.addEventListener('abort', () => {
        if (this.active === result) {
          this.active = undefined;
          this.activeNodeId = undefined;
        }
        close();
      }, { once: true });
      return result;
    } catch (error) {
      close();
      if (sessionId && this.signaling.cancelOffer) {
        const cancelController = new AbortController();
        const cancelTimer = setTimeout(() => cancelController.abort(), 5_000);
        void Promise.resolve()
          .then(() => this.signaling.cancelOffer!(sessionId!, cancelController.signal))
          .catch(() => undefined)
          .finally(() => clearTimeout(cancelTimer));
      }
      throw error;
    }
  }
}


export type HttpClientSignalingOptions = {
  baseUrl: string;
  headers: () => PeerHttpHeaders | Promise<PeerHttpHeaders>;
  fetch?: typeof fetch;
};

/** Fetch-based signaling adapter; authentication and session semantics are supplied by the host. */
export function createHttpClientSignaling(options: HttpClientSignalingOptions): PeerClientSignaling {
  const root = options.baseUrl.replace(/\/$/, '');
  const transport = options.fetch ?? fetch;
  async function request(path: string, init: RequestInit = {}): Promise<unknown> {
    const headers = new Headers(await options.headers());
    if (init.body) headers.set('content-type', 'application/json');
    const response = await transport(root + path, { ...init, headers });
    if (!response.ok) throw new Error('peer_signal_http_' + response.status);
    if (response.status === 204) return undefined;
    return response.json();
  }
  return {
    async getIceServers({ protocol, nodeId, signal }) {
      const params = new URLSearchParams({ protocol });
      if (nodeId) params.set('nodeId', nodeId);
      let value: unknown;
      try {
        value = await request('/ice-config?' + params.toString(), { signal });
      } catch (error) {
        // Compatibility with older signaling servers: host-only first offer, then
        // one fresh offer if /sessions returns STUN/TURN configuration.
        if (error instanceof Error &&
            (error.message === 'peer_signal_http_404' || error.message === 'peer_signal_http_501')) {
          return [];
        }
        throw error;
      }
      return readPeerIceServers((value as { iceServers?: unknown }).iceServers);
    },
    async createOffer({ protocol, offerSdp, nodeId, signal }) {
      const value = await request('/sessions', {
        method: 'POST', signal,
        body: JSON.stringify({ protocol, offerSdp, ...(nodeId ? { nodeId } : {}) }),
      });
      if (!value || typeof (value as PeerSessionCreation).sessionId !== 'string') throw new Error('invalid_peer_session');
      const row = value as PeerSessionCreation;
      return { sessionId: row.sessionId, iceServers: readPeerIceServers(row.iceServers) };
    },
    async readAnswer(sessionId, signal) {
      const value = await request('/sessions/' + encodeURIComponent(sessionId), { signal });
      if (!value || typeof value !== 'object' || Array.isArray(value)) throw new Error('invalid_peer_answer');
      const status = value as { status?: unknown; answerSdp?: unknown };
      if (status.status === 'pending' || status.status === 'failed') {
        if (status.answerSdp !== undefined) throw new Error('invalid_peer_answer');
        return { status: status.status };
      }
      if (status.status === 'answered' && typeof status.answerSdp === 'string' && status.answerSdp.length > 0) {
        return { status: 'answered', answerSdp: status.answerSdp };
      }
      throw new Error('invalid_peer_answer');
    },
    async acknowledgeOffer(sessionId, signal) {
      await request('/sessions/' + encodeURIComponent(sessionId) + '/ack', { method: 'POST', signal });
    },
    async cancelOffer(sessionId, signal) {
      await request('/sessions/' + encodeURIComponent(sessionId), { method: 'DELETE', signal });
    },
  };
}
