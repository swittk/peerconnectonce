import { RTCPeerConnection, type RTCDataChannel } from 'werift';
import type { PeerIceServer, PeerHttpHeaders } from '../shared/contracts.js';
import {
  isPeerLabel, type PeerNodeSignaling, type PeerOffer,
} from '../shared/contracts.js';

export type { PeerNodeSignaling, PeerOffer } from '../shared/contracts.js';

export type PeerChannelWriter = {
  sendText(message: string): Promise<void>;
  sendBinary(message: Uint8Array): Promise<void>;
  close(): void;
};

export type PeerServiceContext = {
  readonly ownerUserId: string;
  readonly protocol: string;
  readonly nodeId: string;
  readonly signal: AbortSignal;
  readonly writer: PeerChannelWriter;
  /** Keep a long-running stream alive independently of the default idle timer. */
  beginWork(): () => void;
};

export type PeerServiceSession = {
  receiveMessage(message: string | Buffer): void | Promise<void>;
  close(): void | Promise<void>;
  /** Optional for adapters whose work outlives receiveMessage (e.g. an HTTP response reader). */
  hasActiveWork?(): boolean;
};

export type PeerServiceRegistration = {
  label: string;
  create(context: PeerServiceContext): PeerServiceSession;
};

export type PeerServerOptions = {
  signaling: PeerNodeSignaling;
  services: readonly PeerServiceRegistration[];
  iceServers: PeerIceServer[];
  pollIntervalMs?: number;
  maxPeers?: number;
  setupTimeoutMs?: number;
  idleTimeoutMs?: number;
  /** Buffered SCTP high/low cuts, not a throughput throttle. */
  bufferedHighBytes?: number;
  bufferedLowBytes?: number;
  /** Bounds unsent messages held by our promise queue. */
  maxQueuedBytes?: number;
  /** Independent entry-count bound even for empty data-channel messages. */
  maxQueuedMessages?: number;
  maxMessageBytes?: number;
  /** Opt-in QoS only; undefined/0 runs without artificial bandwidth pacing. */
  outgoingBytesPerSecond?: number;
  onError?: (error: Error) => void;
};

/** Versioned/reliable channel metadata; callers need no Werift-specific declarations. */
export type PeerChannelDescriptor = {
  readonly label: string;
  readonly ordered: boolean;
  readonly maxRetransmits?: number | null;
  readonly maxPacketLifeTime?: number | null;
};

/** A generic service allowlist. No arbitrary browser label can invoke a local service. */
export class PeerServiceRegistry {
  private readonly registrations: Map<string, PeerServiceRegistration>;

  constructor(services: readonly PeerServiceRegistration[]) {
    if (services.length === 0 || services.length > 32) throw new Error('invalid_peer_services');
    this.registrations = new Map();
    for (const service of services) {
      if (!isPeerLabel(service.label) || this.registrations.has(service.label)) {
        throw new Error('invalid_peer_service_registration');
      }
      this.registrations.set(service.label, service);
    }
  }

  has(label: string): boolean { return this.registrations.has(label); }

  create(channel: PeerChannelDescriptor, context: PeerServiceContext): PeerServiceSession {
    if (channel.label !== context.protocol || !this.registrations.has(channel.label) ||
        !channel.ordered || channel.maxRetransmits != null || channel.maxPacketLifeTime != null) {
      throw new Error('unregistered_or_unreliable_peer_channel');
    }
    return this.registrations.get(channel.label)!.create(context);
  }
}

export type PeerWriterOptions = {
  highBytes?: number;
  lowBytes?: number;
  maxQueuedBytes?: number;
  /** Independent entry-count bound even for empty data-channel messages. */
  maxQueuedMessages?: number;
  maxMessageBytes?: number;
  outgoingBytesPerSecond?: number;
  signal?: AbortSignal;
};

/** Minimal writable Node data-channel surface; no Werift types leak into public .d.ts. */
type WritableChannel = {
  readonly readyState: 'connecting' | 'open' | 'closing' | 'closed';
  readonly bufferedAmount: number;
  bufferedAmountLowThreshold: number;
  readonly bufferedAmountLow: {
    subscribe(callback: () => void): { unSubscribe(): void };
  };
  send(message: string | Buffer): void;
  close(): void;
};

/** Bounded writer shared by every service. No default outgoing rate limiter. */
export class BoundedPeerWriter implements PeerChannelWriter {
  private tail = Promise.resolve();
  private queuedBytes = 0;
  private queuedMessages = 0;
  private closed = false;
  private readonly stop = new AbortController();
  private nextSendAt = 0;
  private readonly high: number;
  private readonly low: number;
  private readonly maxQueued: number;
  private readonly maxQueuedMessages: number;
  private readonly maxFrame: number;
  private readonly rate: number;

  constructor(private readonly channel: WritableChannel, options: PeerWriterOptions = {}) {
    this.high = options.highBytes ?? 4 * 1024 * 1024;
    this.low = options.lowBytes ?? 1024 * 1024;
    this.maxQueued = options.maxQueuedBytes ?? 8 * 1024 * 1024;
    this.maxQueuedMessages = options.maxQueuedMessages ?? 1024;
    this.maxFrame = options.maxMessageBytes ?? 64 * 1024;
    this.rate = options.outgoingBytesPerSecond ?? 0;
    if (this.high <= this.low || this.low < 0 || this.maxQueued <= 0 || this.maxFrame <= 0 ||
        !Number.isSafeInteger(this.maxQueuedMessages) || this.maxQueuedMessages < 1 ||
        !Number.isFinite(this.rate) || this.rate < 0) throw new Error('invalid_peer_writer_limits');
    channel.bufferedAmountLowThreshold = this.low;
    options.signal?.addEventListener('abort', () => this.close(), { once: true });
  }

  sendText(message: string): Promise<void> {
    return this.enqueue(message, Buffer.byteLength(message));
  }

  sendBinary(message: Uint8Array): Promise<void> {
    return this.enqueue(Buffer.from(message.buffer, message.byteOffset, message.byteLength), message.byteLength);
  }

  private enqueue(message: string | Buffer, size: number): Promise<void> {
    if (this.closed || this.channel.readyState !== 'open') return Promise.reject(new Error('peer_channel_closed'));
    if (size > this.maxFrame || this.queuedBytes + size > this.maxQueued ||
        this.queuedMessages >= this.maxQueuedMessages) {
      return Promise.reject(new Error('peer_writer_capacity'));
    }
    this.queuedBytes += size;
    this.queuedMessages++;
    const send = this.tail.then(async () => {
      if (this.closed) throw new Error('peer_channel_closed');
      await this.waitForBuffer();
      if (this.rate > 0) await this.waitForOptionalRate(size);
      if (this.closed || this.channel.readyState !== 'open') throw new Error('peer_channel_closed');
      this.channel.send(message);
    }).finally(() => {
      this.queuedBytes -= size;
      this.queuedMessages--;
    });
    this.tail = send.catch(() => undefined);
    return send;
  }

  private async waitForBuffer(): Promise<void> {
    while (this.channel.bufferedAmount > this.high) {
      if (this.closed || this.channel.readyState !== 'open') throw new Error('peer_channel_closed');
      await new Promise<void>((resolve) => {
        let settled = false;
        let subscription: { unSubscribe(): void } | undefined;
        const finish = () => {
          if (settled) return;
          settled = true;
          clearTimeout(timeout);
          this.stop.signal.removeEventListener('abort', finish);
          subscription?.unSubscribe();
          resolve();
        };
        const timeout = setTimeout(finish, 250);
        this.stop.signal.addEventListener('abort', finish, { once: true });
        subscription = this.channel.bufferedAmountLow.subscribe(finish);
        if (settled) subscription.unSubscribe();
      });
    }
  }

  /** Optional operator pacing, with one cancellable timer only when a real delay is needed. */
  private async waitForOptionalRate(bytes: number): Promise<void> {
    if (this.closed) throw new Error('peer_channel_closed');
    const now = performance.now();
    const at = Math.max(now, this.nextSendAt);
    // Accumulate fractional waits instead of scheduling a timer per small frame.
    if (at - now >= 8) {
      await new Promise<void>((resolve) => {
        let finished = false;
        const end = () => {
          if (finished) return;
          finished = true;
          clearTimeout(timer);
          this.stop.signal.removeEventListener('abort', end);
          resolve();
        };
        const timer = setTimeout(end, at - now);
        this.stop.signal.addEventListener('abort', end, { once: true });
        if (this.stop.signal.aborted) end();
      });
    }
    if (this.closed) throw new Error('peer_channel_closed');
    this.nextSendAt = at + (bytes / this.rate) * 1000;
  }

  close(): void {
    if (this.closed) return;
    this.closed = true;
    this.stop.abort();
    if (this.channel.readyState === 'open' || this.channel.readyState === 'connecting') this.channel.close();
  }
}

function delay(ms: number, signal: AbortSignal): Promise<void> {
  if (signal.aborted) return Promise.resolve();
  return new Promise((resolve) => {
    const end = () => { clearTimeout(timer); signal.removeEventListener('abort', end); resolve(); };
    const timer = setTimeout(end, ms);
    signal.addEventListener('abort', end, { once: true });
  });
}

function beforeDeadline<T>(promise: Promise<T>, deadline: number, signal: AbortSignal): Promise<T> {
  if (signal.aborted) return Promise.reject(new Error('peer_stopped'));
  const remaining = deadline - Date.now();
  if (remaining <= 0) return Promise.reject(new Error('peer_setup_timeout'));
  return new Promise((resolve, reject) => {
    let settled = false;
    let timer: ReturnType<typeof setTimeout>;
    const cleanup = () => { clearTimeout(timer); signal.removeEventListener('abort', abort); };
    const finish = (error?: Error, result?: T) => {
      if (settled) return;
      settled = true;
      cleanup();
      if (error) reject(error); else resolve(result as T);
    };
    const abort = () => finish(new Error('peer_stopped'));
    signal.addEventListener('abort', abort, { once: true });
    timer = setTimeout(() => finish(new Error('peer_setup_timeout')), remaining);
    promise.then((result) => finish(undefined, result), (reason: unknown) =>
      finish(reason instanceof Error ? reason : new Error('peer_setup_failed')));
  });
}

type RunningPeer = { close(): Promise<void>; readonly finished: Promise<void> };

export class PeerServer {
  private readonly registry: PeerServiceRegistry;
  private readonly options: Required<Pick<PeerServerOptions,
    'pollIntervalMs' | 'maxPeers' | 'setupTimeoutMs' | 'idleTimeoutMs'
  >> & PeerServerOptions;
  private readonly livePeers = new Map<string, RunningPeer>();
  private running = false;

  constructor(options: PeerServerOptions) {
    this.registry = new PeerServiceRegistry(options.services);
    this.options = {
      ...options,
      pollIntervalMs: options.pollIntervalMs ?? 1000,
      maxPeers: options.maxPeers ?? 16,
      setupTimeoutMs: options.setupTimeoutMs ?? 45_000,
      idleTimeoutMs: options.idleTimeoutMs ?? 15 * 60_000,
    };
    if (this.options.maxPeers < 1 || this.options.pollIntervalMs < 1 ||
        this.options.idleTimeoutMs < 1 || this.options.setupTimeoutMs < 1)
      throw new Error('invalid_peer_server_limits');
  }

  /** Outbound-only node polling. The host controls credentials and the signaling endpoint. */
  async start(shutdownSignal: AbortSignal): Promise<void> {
    if (this.running) throw new Error('peer_server_already_running');
    this.running = true;
    try {
      while (!shutdownSignal.aborted) {
        if (this.livePeers.size >= this.options.maxPeers) {
          await delay(this.options.pollIntervalMs, shutdownSignal);
          continue;
        }
        try {
          const offer = await this.options.signaling.claim(shutdownSignal);
          if (offer) {
            if (!this.registry.has(offer.protocol)) {
              await this.options.signaling.fail(offer.sessionId, shutdownSignal);
              continue;
            }
            const peer = await this.accept(offer, shutdownSignal);
            this.livePeers.set(offer.sessionId, peer);
            void peer.finished.finally(() => {
              this.livePeers.delete(offer.sessionId);
            });
          } else {
            await delay(this.options.pollIntervalMs, shutdownSignal);
          }
        } catch (error) {
          if (!shutdownSignal.aborted) {
            this.options.onError?.(error instanceof Error ? error : new Error('peer_error'));
            await delay(this.options.pollIntervalMs, shutdownSignal);
          }
        }
      }
    } finally {
      this.running = false;
      const closes: Promise<void>[] = [];
      for (const peer of this.livePeers.values()) {
        closes.push(peer.close().catch(() => undefined));
      }
      await Promise.all(closes);
    }
  }

  /** Also usable by an onsite backend without starting an outbound polling loop. */
  async accept(offer: PeerOffer, shutdownSignal: AbortSignal): Promise<RunningPeer> {
    if (shutdownSignal.aborted) throw new Error('peer_setup_stopped');
    if (!this.registry.has(offer.protocol)) throw new Error('unregistered_peer_service');
    const controller = new AbortController();
    const connection = new RTCPeerConnection({
      iceServers: this.options.iceServers,
      iceUseDefaultStunServer: false,
      maxMessageSize: this.options.maxMessageBytes ?? 64 * 1024,
    });
    let activeWork = 0;
    let lastActivity = Date.now();
    let channel: RTCDataChannel | undefined;
    let session: PeerServiceSession | undefined;
    let alreadyClosed = false;
    let finishedCallback: () => void = () => {};
    const finished = new Promise<void>((resolve) => { finishedCallback = resolve; });
    const markWork = () => {
      activeWork += 1;
      lastActivity = Date.now();
      let released = false;
      return () => {
        if (released) return;
        released = true;
        activeWork -= 1;
        lastActivity = Date.now();
      };
    };
    let idleCheck: ReturnType<typeof setInterval> | undefined;
    let queuedIncomingBytes = 0;
    let queuedIncomingMessages = 0;
    let incomingTail = Promise.resolve();
    const maxIncomingQueueBytes = this.options.maxQueuedBytes ?? 8 * 1024 * 1024;
    const maxIncomingMessages = this.options.maxQueuedMessages ?? 1024;
    const maxIncomingFrameBytes = this.options.maxMessageBytes ?? 64 * 1024;
    // The existing finished promise doubles as the one idempotent close promise:
    // concurrent shutdown callers must await the SAME cleanup, not return early.
    const close = (): Promise<void> => {
      if (alreadyClosed) return finished;
      alreadyClosed = true;
      controller.abort();
      clearInterval(idleCheck);
      shutdownSignal.removeEventListener('abort', onShutdown);
      void (async () => {
        try {
          try { await session?.close(); } catch { /* best-effort service cleanup */ }
          try { if (channel?.readyState === 'open') channel.close(); } catch { /* already closed */ }
          try { await connection.close(); } catch { /* already closed */ }
        } finally {
          finishedCallback();
        }
      })();
      return finished;
    };
    const onShutdown = () => void close();
    shutdownSignal.addEventListener('abort', onShutdown, { once: true });
    if (shutdownSignal.aborted) void close();
    connection.connectionStateChange.subscribe((state) => {
      if (state === 'failed' || state === 'closed') void close();
    });
    connection.onDataChannel.subscribe((openedChannel) => {
      if (channel || openedChannel.label !== offer.protocol) {
        openedChannel.close();
        void close();
        return;
      }
      channel = openedChannel;
      const writer = new BoundedPeerWriter(channel, {
        highBytes: this.options.bufferedHighBytes,
        lowBytes: this.options.bufferedLowBytes,
        maxQueuedBytes: this.options.maxQueuedBytes,
        maxQueuedMessages: this.options.maxQueuedMessages,
        maxMessageBytes: this.options.maxMessageBytes,
        outgoingBytesPerSecond: this.options.outgoingBytesPerSecond,
        signal: controller.signal,
      });
      try {
        session = this.registry.create(channel, {
          ownerUserId: offer.ownerUserId, protocol: offer.protocol,
          nodeId: offer.nodeId, signal: controller.signal, writer,
          beginWork: markWork,
        });
      } catch {
        writer.close();
        void close();
        return;
      }
      // Ordered transport must also preserve service execution order after asynchronous writes.
      // Bound retained JS payloads independently of SCTP bufferedAmount.
      openedChannel.onMessage.subscribe((message) => {
        if (alreadyClosed || controller.signal.aborted) return;
        const bytes = typeof message === 'string' ? Buffer.byteLength(message) : message.byteLength;
        if (bytes > maxIncomingFrameBytes || queuedIncomingBytes + bytes > maxIncomingQueueBytes ||
            queuedIncomingMessages >= maxIncomingMessages) {
          void close();
          return;
        }
        queuedIncomingBytes += bytes;
        queuedIncomingMessages++;
        const done = markWork();
        const next = incomingTail.then(() => {
          if (alreadyClosed || controller.signal.aborted) return;
          return session?.receiveMessage(message);
        });
        incomingTail = next.catch(() => { void close(); }).then(() => {
          queuedIncomingBytes -= bytes;
          queuedIncomingMessages--;
          done();
        });
      });
      openedChannel.stateChanged.subscribe((state) => {
        if (state === 'open') lastActivity = Date.now();
        if (state === 'closed') void close();
      });
      if (openedChannel.readyState === 'open') lastActivity = Date.now();
    });

    try {
      const deadline = Date.now() + this.options.setupTimeoutMs;
      await beforeDeadline(connection.setRemoteDescription({ type: 'offer', sdp: offer.offerSdp }), deadline, controller.signal);
      const answer = await beforeDeadline(connection.createAnswer(), deadline, controller.signal);
      await beforeDeadline(connection.setLocalDescription(answer), deadline, controller.signal);
      const sdp = connection.localDescription?.sdp;
      if (!sdp) throw new Error('peer_answer_missing');
      await beforeDeadline(this.options.signaling.answer(offer.sessionId, sdp, controller.signal), deadline, controller.signal);
    } catch (error) {
      await close();
      if (!shutdownSignal.aborted) await this.options.signaling.fail(offer.sessionId, shutdownSignal).catch(() => undefined);
      throw error;
    }

    // A peer may have closed while its answer was being published. Never leak an
    // idle interval after cleanup already ran.
    if (alreadyClosed || controller.signal.aborted) {
      await close();
      throw new Error('peer_setup_stopped');
    }
    idleCheck = setInterval(() => {
      if (!channel || !session || channel.readyState !== 'open') {
        // Setup is not inactivity: the peer is not idle until its service channel opens.
        if (Date.now() - lastActivity >= this.options.setupTimeoutMs) void close();
        return;
      }
      if (activeWork > 0 || session.hasActiveWork?.()) return;
      if (Date.now() - lastActivity >= this.options.idleTimeoutMs) void close();
    }, Math.min(1000, this.options.idleTimeoutMs));
    return { close, finished };
  }
}

export { PeerServer as PeerGatewayOnce };

export type HttpNodeSignalingOptions = {
  baseUrl: string;
  nodeId: string;
  /** Host-provided Fetch headers, without DOM-only HeadersInit declarations. */
  headers: () => PeerHttpHeaders | Promise<PeerHttpHeaders>;
  fetch?: typeof fetch;
  requestTimeoutMs?: number;
};

export function createHttpNodeSignaling(options: HttpNodeSignalingOptions): PeerNodeSignaling {
  const fetchImpl = options.fetch ?? fetch;
  const base = options.baseUrl.replace(/\/$/, '');
  async function request(
    path: string, signal?: AbortSignal, body?: object, expectJson = false
  ): Promise<{ status: number; payload?: unknown }> {
    const controller = new AbortController();
    const stop = () => controller.abort();
    signal?.addEventListener('abort', stop, { once: true });
    if (signal?.aborted) controller.abort();
    const timer = setTimeout(stop, options.requestTimeoutMs ?? 15_000);
    try {
      // Credential refresh may be async and may ignore AbortSignal. Race it
      // against the existing request controller so shutdown/timeout settles
      // promptly and cannot proceed to fetch with stale credentials.
      const headerValues = await new Promise<PeerHttpHeaders>((resolve, reject) => {
        if (controller.signal.aborted) {
          reject(new Error('peer_request_aborted'));
          return;
        }
        const onAbort = () => {
          controller.signal.removeEventListener('abort', onAbort);
          reject(new Error('peer_request_aborted'));
        };
        controller.signal.addEventListener('abort', onAbort, { once: true });
        Promise.resolve().then(() => {
          if (controller.signal.aborted) throw new Error('peer_request_aborted');
          return options.headers();
        }).then(
          (value) => {
            controller.signal.removeEventListener('abort', onAbort);
            if (controller.signal.aborted) reject(new Error('peer_request_aborted'));
            else resolve(value);
          },
          (error: unknown) => {
            controller.signal.removeEventListener('abort', onAbort);
            reject(error);
          }
        );
      });
      const headers = new Headers(headerValues);
      if (body) headers.set('content-type', 'application/json');
      const response = await fetchImpl(base + path, {
        method: body ? 'POST' : 'GET', headers, signal: controller.signal,
        ...(body ? { body: JSON.stringify(body) } : {}),
      });
      // Keep timeout/shutdown attached while consuming the response body, not
      // merely while the remote endpoint is sending headers.
      const payload = expectJson && response.ok && response.status !== 204
        ? await response.json() as unknown : undefined;
      return { status: response.status, payload };
    } finally {
      clearTimeout(timer);
      signal?.removeEventListener('abort', stop);
    }
  }
  return {
    async claim(signal) {
      const response = await request('/claim?nodeId=' + encodeURIComponent(options.nodeId), signal, undefined, true);
      if (response.status === 204) return undefined;
      if (response.status < 200 || response.status >= 300) throw new Error('peer_claim_http_' + response.status);
      const offer = response.payload as PeerOffer;
      if (!offer || typeof offer !== 'object' || offer.nodeId !== options.nodeId || !isPeerLabel(offer.protocol) ||
          typeof offer.offerSdp !== 'string' || !offer.sessionId || !offer.ownerUserId)
        throw new Error('peer_claim_invalid');
      return offer;
    },
    async answer(sessionId, answerSdp, signal) {
      const response = await request('/answer', signal, { sessionId, answerSdp });
      if (response.status !== 204) throw new Error('peer_answer_http_' + response.status);
    },
    async fail(sessionId, signal) {
      const response = await request('/answer', signal, { sessionId, failed: true });
      if (response.status !== 204 && response.status !== 409) throw new Error('peer_fail_http_' + response.status);
    },
  };
}
