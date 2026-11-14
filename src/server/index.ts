import { RTCPeerConnection, type RTCDataChannel } from 'werift';
import type { PeerIceServer, PeerHttpHeaders } from '../shared/contracts.js';
import {
  isPeerLabel, type PeerNodeSignaling, type PeerOffer,
} from '../shared/contracts.js';
import { resolvePeerHeaders } from '../shared/http.js';

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
  /** Maximum wait for an adapter's cleanup before releasing its peer slot. */
  serviceCloseTimeoutMs?: number;
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
    if (!Number.isSafeInteger(this.high) || this.high <= 0 ||
        !Number.isSafeInteger(this.low) || this.low < 0 || this.high <= this.low ||
        !Number.isSafeInteger(this.maxQueued) || this.maxQueued <= 0 ||
        !Number.isSafeInteger(this.maxFrame) || this.maxFrame <= 0 ||
        !Number.isSafeInteger(this.maxQueuedMessages) || this.maxQueuedMessages < 1 ||
        !Number.isFinite(this.rate) || this.rate < 0) throw new Error('invalid_peer_writer_limits');
    channel.bufferedAmountLowThreshold = this.low;
    options.signal?.addEventListener('abort', () => this.close(), { once: true });
    if (options.signal?.aborted) this.close();
  }

  sendText(message: string): Promise<void> {
    return this.enqueue(message, Buffer.byteLength(message));
  }

  sendBinary(message: Uint8Array): Promise<void> {
    return this.enqueue(message, message.byteLength);
  }

  private enqueue(message: string | Uint8Array, size: number): Promise<void> {
    if (this.closed || this.channel.readyState !== 'open') return Promise.reject(new Error('peer_channel_closed'));
    if (size > this.maxFrame || size > this.maxQueued - this.queuedBytes ||
        this.queuedMessages >= this.maxQueuedMessages) {
      return Promise.reject(new Error('peer_writer_capacity'));
    }
    // Copy exactly the admitted visible bytes, and ONLY after admission. A
    // 1 KiB slice cannot retain its 64 MiB backing array while queued, nor
    // can a rejected oversized payload cause an unnecessary allocation.
    const payload = typeof message === 'string' ? message : Buffer.from(message);
    this.queuedBytes += size;
    this.queuedMessages++;
    const send = this.tail.then(async () => {
      if (this.closed) throw new Error('peer_channel_closed');
      await this.waitForBuffer();
      if (this.rate > 0) await this.waitForOptionalRate(size);
      if (this.closed || this.channel.readyState !== 'open') throw new Error('peer_channel_closed');
      this.channel.send(payload);
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

function beforeDeadline<T>(run: () => Promise<T>, deadline: number, signal: AbortSignal): Promise<T> {
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
    Promise.resolve().then(() => {
      if (signal.aborted) throw new Error('peer_stopped');
      return run();
    }).then((result) => finish(undefined, result), (reason: unknown) =>
      finish(reason instanceof Error ? reason : new Error('peer_setup_failed')));
  });
}

type RunningPeer = { close(): Promise<void>; readonly finished: Promise<void> };

export class PeerServer {
  private readonly registry: PeerServiceRegistry;
  private readonly options: Required<Pick<PeerServerOptions,
    'pollIntervalMs' | 'maxPeers' | 'setupTimeoutMs' | 'idleTimeoutMs' | 'serviceCloseTimeoutMs'
  >> & PeerServerOptions;
  private readonly livePeers = new Map<string, RunningPeer>();
  /** Reservations include peers still negotiating; Set admission is O(1). */
  private readonly reservedPeerIds = new Set<string>();
  private running = false;

  constructor(options: PeerServerOptions) {
    this.registry = new PeerServiceRegistry(options.services);
    this.options = {
      ...options,
      pollIntervalMs: options.pollIntervalMs ?? 1000,
      maxPeers: options.maxPeers ?? 16,
      setupTimeoutMs: options.setupTimeoutMs ?? 45_000,
      idleTimeoutMs: options.idleTimeoutMs ?? 15 * 60_000,
      serviceCloseTimeoutMs: options.serviceCloseTimeoutMs ?? 5_000,
    };
    if (!Number.isSafeInteger(this.options.maxPeers) || this.options.maxPeers < 1 ||
        !Number.isSafeInteger(this.options.pollIntervalMs) || this.options.pollIntervalMs < 1 ||
        !Number.isSafeInteger(this.options.idleTimeoutMs) || this.options.idleTimeoutMs < 1 ||
        !Number.isSafeInteger(this.options.setupTimeoutMs) || this.options.setupTimeoutMs < 1 ||
        !Number.isSafeInteger(this.options.serviceCloseTimeoutMs) || this.options.serviceCloseTimeoutMs < 1 ||
        this.options.pollIntervalMs > 2_147_483_647 ||
        this.options.idleTimeoutMs > 2_147_483_647 ||
        this.options.setupTimeoutMs > 2_147_483_647 ||
        this.options.serviceCloseTimeoutMs > 2_147_483_647)
      throw new Error('invalid_peer_server_limits');
    // Fail fast on invalid writer bounds before accepting an incoming peer.
    const high = options.bufferedHighBytes ?? 4 * 1024 * 1024;
    const low = options.bufferedLowBytes ?? 1024 * 1024;
    const queued = options.maxQueuedBytes ?? 8 * 1024 * 1024;
    const messages = options.maxQueuedMessages ?? 1024;
    const frame = options.maxMessageBytes ?? 64 * 1024;
    if (!Number.isSafeInteger(high) || high <= 0 ||
        !Number.isSafeInteger(low) || low < 0 || low >= high ||
        !Number.isSafeInteger(queued) || queued <= 0 ||
        !Number.isSafeInteger(messages) || messages < 1 ||
        !Number.isSafeInteger(frame) || frame <= 0 ||
        !Number.isFinite(options.outgoingBytesPerSecond ?? 0) ||
        (options.outgoingBytesPerSecond ?? 0) < 0)
      throw new Error('invalid_peer_writer_limits');
  }

  /** Outbound-only node polling. The host controls credentials and the signaling endpoint. */
  async start(shutdownSignal: AbortSignal): Promise<void> {
    if (this.running) throw new Error('peer_server_already_running');
    this.running = true;
    try {
      while (!shutdownSignal.aborted) {
        if (this.reservedPeerIds.size >= this.options.maxPeers) {
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
            try {
              await this.accept(offer, shutdownSignal);
            } catch (error) {
              // Direct admission can fill the last slot while claim() awaits.
              // Notify that already-claimed offer instead of leaving it pending.
              if (error instanceof Error && error.message === 'peer_capacity') {
                await this.options.signaling.fail(offer.sessionId, shutdownSignal).catch(() => undefined);
              }
              throw error;
            }
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
    if (this.reservedPeerIds.size >= this.options.maxPeers ||
        this.reservedPeerIds.has(offer.sessionId)) throw new Error('peer_capacity');
    // Reserve synchronously before the first await, shared by direct accept()
    // and the outbound claim loop. Failed setup/close releases the reservation.
    this.reservedPeerIds.add(offer.sessionId);
    let connection: RTCPeerConnection;
    try {
      connection = new RTCPeerConnection({
        iceServers: this.options.iceServers,
        iceUseDefaultStunServer: false,
        maxMessageSize: this.options.maxMessageBytes ?? 64 * 1024,
      });
    } catch (error) {
      this.reservedPeerIds.delete(offer.sessionId);
      throw error;
    }
    const controller = new AbortController();
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
          if (session) {
            let closeTimer: ReturnType<typeof setTimeout> | undefined;
            try {
              await Promise.race([
                Promise.resolve().then(() => session!.close()),
                new Promise<void>((resolve) => {
                  closeTimer = setTimeout(resolve, this.options.serviceCloseTimeoutMs);
                }),
              ]);
            } catch { /* best-effort service cleanup */ }
            finally { clearTimeout(closeTimer); }
          }
          try { if (channel?.readyState === 'open') channel.close(); } catch { /* already closed */ }
          try { await connection.close(); } catch { /* already closed */ }
        } finally {
          this.livePeers.delete(offer.sessionId);
          this.reservedPeerIds.delete(offer.sessionId);
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
      await beforeDeadline(() => connection.setRemoteDescription({ type: 'offer', sdp: offer.offerSdp }), deadline, controller.signal);
      const answer = await beforeDeadline(() => connection.createAnswer(), deadline, controller.signal);
      await beforeDeadline(() => connection.setLocalDescription(answer), deadline, controller.signal);
      const sdp = connection.localDescription?.sdp;
      if (!sdp) throw new Error('peer_answer_missing');
      await beforeDeadline(() => this.options.signaling.answer(offer.sessionId, sdp, controller.signal), deadline, controller.signal);
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
      if (activeWork > 0) return;
      try {
        if (session.hasActiveWork?.()) return;
      } catch {
        // A malformed adapter must not throw out of a timer and crash Node.
        // Match receiveMessage's failure isolation by closing only this peer.
        void close();
        return;
      }
      if (Date.now() - lastActivity >= this.options.idleTimeoutMs) void close();
    }, Math.min(1000, this.options.idleTimeoutMs));
    const running = { close, finished };
    this.livePeers.set(offer.sessionId, running);
    return running;
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
  const requestTimeoutMs = options.requestTimeoutMs ?? 15_000;
  if (!Number.isSafeInteger(requestTimeoutMs) || requestTimeoutMs < 1 || requestTimeoutMs > 2_147_483_647)
    throw new Error('invalid_peer_signaling_limits');
  const fetchImpl = options.fetch ?? fetch;
  const base = options.baseUrl.replace(/\/$/, '');
  async function request(
    path: string, signal?: AbortSignal, body?: object, expectJson = false
  ): Promise<{ status: number; payload?: unknown }> {
    const controller = new AbortController();
    const stop = () => controller.abort();
    signal?.addEventListener('abort', stop, { once: true });
    if (signal?.aborted) controller.abort();
    const timer = setTimeout(stop, requestTimeoutMs);
    try {
      const headers = await resolvePeerHeaders(options.headers, controller.signal);
      if (body) headers.set('content-type', 'application/json');
      // A shutdown microtask can fire after credential resolution but before
      // this continuation runs. Do not dispatch an already-aborted request,
      // even with custom Fetch implementations that ignore AbortSignal.
      if (controller.signal.aborted) throw new Error('peer_request_aborted');
      const response = await fetchImpl(base + path, {
        method: body ? 'POST' : 'GET', headers, signal: controller.signal,
        ...(body ? { body: JSON.stringify(body) } : {}),
      });
      // Keep timeout/shutdown attached through body handling. Parsed JSON
      // consumes its stream; every other response must cancel its unread body
      // to release sockets and flow-control credit instead of relying on GC.
      const consumeJson = expectJson && response.ok && response.status !== 204;
      try {
        const payload = consumeJson ? await response.json() as unknown : undefined;
        return { status: response.status, payload };
      } finally {
        if (!consumeJson) {
          try { await response.body?.cancel(); } catch { /* preserve HTTP status */ }
        }
      }
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
