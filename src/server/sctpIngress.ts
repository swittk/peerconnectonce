/**
 * Receive-side admission for Werift 0.27 SCTP associations.
 *
 * WebRTC/DataChannel message events fire only after SCTP fragment reassembly.
 * An attacker can otherwise retain arbitrary incomplete fragments inside the
 * association before the application's per-frame checks run. This adapter
 * enforces the receive window and per-message/fragment limits at DATA ingress.
 *
 * The current Werift SCTP receive surface is intentionally checked strictly:
 * incompatible future implementations must fail closed, not disable the guard.
 */
type DataChunk = {
  tsn: number;
  streamId: number;
  streamSeqNum: number;
  flags: number;
  userData: Buffer;
};
type Subscription = { unSubscribe(): void };
type MessageEvent = { subscribe(callback: (streamId: number) => void): Subscription };
type ResetEvent = { subscribe(callback: (ids: number[]) => void): Subscription };
type InboundStream = { streamSequenceNumber: number };
type Association = {
  advertisedRwnd: number;
  lastReceivedTsn?: number;
  sackMisOrdered: Set<number>;
  receiveDataChunk(chunk: DataChunk): void;
  onReceive: MessageEvent;
  onReconfigStreams: ResetEvent;
  inboundStreams: Record<number, InboundStream>;
};
export type WeriftSctpReceiveSurface = {
  readonly sctp: { readonly sctp: Association } | null;
};
type TrackedMessage = { bytes: number; fragments: number; tsns: Set<number> };
type TrackedStream = Map<number, TrackedMessage>;
type Fragment = { streamId: number; streamSeqNum: number; flags: number };
const DATA_FIRST = 0x02;
const DATA_LAST = 0x01;
const DATA_UNORDERED = 0x04; // Peer services require ordered channels.

/**
 * Werift 0.27 reassembles fragments adjacent in TSN order without checking
 * their SSNs. Enforce SCTP DATA boundaries so distinct SSNs cannot masquerade
 * as separate messages while Werift would combine their payloads.
 * Called only on the two neighboring TSNs, never a scan of reassembly.
 */
function validBoundary(before: Fragment, after: Fragment): boolean {
  if (before.streamId !== after.streamId) return true;
  const beforeEnds = (before.flags & DATA_LAST) !== 0;
  const afterStarts = (after.flags & DATA_FIRST) !== 0;
  // A new message begins only when the previous message actually ended.
  if (beforeEnds !== afterStarts) return false;
  return beforeEnds || before.streamSeqNum === after.streamSeqNum;
}
const MAX_PARTS_PER_MESSAGE = 256;
const MAX_TRACKED_FRAGMENTS = 2048;
const MAX_TRACKED_STREAMS = 16;

/** Operates once per SCTP association, never on completed user-data messages. */
export function protectWeriftSctpReceive(
  peer: WeriftSctpReceiveSurface,
  maxMessageBytes: number,
  onViolation: () => void,
): () => void {
  const association = peer.sctp?.sctp;
  if (!association ||
      typeof association.receiveDataChunk !== 'function' ||
      typeof association.onReceive?.subscribe !== 'function' ||
      typeof association.onReconfigStreams?.subscribe !== 'function' ||
      !association.inboundStreams ||
      typeof association.sackMisOrdered?.has !== 'function' ||
      !Number.isSafeInteger(association.advertisedRwnd) ||
      association.advertisedRwnd <= 0) {
    throw new Error('peer_sctp_ingress_unsupported');
  }
  if (!Number.isSafeInteger(maxMessageBytes) || maxMessageBytes <= 0) {
    throw new Error('invalid_peer_writer_limits');
  }
  const original = association.receiveDataChunk;
  const maxRetainedBytes = association.advertisedRwnd;
  const tracked = new Map<number, TrackedStream>();
  /** Each accepted incomplete DATA chunk is reachable by TSN in O(1). */
  const fragmentsByTsn = new Map<number, Fragment>();
  let retainedBytes = 0;
  let retainedFragments = 0;
  let blocked = false;
  let cleaned = false;

  function forgetStream(streamId: number): void {
    const messages = tracked.get(streamId);
    if (!messages) return;
    for (const item of messages.values()) {
      retainedFragments -= item.fragments;
      retainedBytes -= item.bytes;
      for (const tsn of item.tsns) fragmentsByTsn.delete(tsn);
    }
    tracked.delete(streamId);
  }

  function violation(): void {
    if (blocked) return;
    // Fail closed on overflow. PeerServer closes this transport asynchronously;
    // do not pass the rejected DATA chunk into Werift's reassembly.
    blocked = true;
    onViolation();
  }

  const messageSub = association.onReceive.subscribe((streamId) => {
    if (blocked) return;
    const stream = association.inboundStreams[streamId];
    if (!stream) return;
    const justDeliveredSeq = (stream.streamSequenceNumber - 1) & 0xffff;
    const messages = tracked.get(streamId);
    const item = messages?.get(justDeliveredSeq);
    if (!item) return;
    retainedFragments -= item.fragments;
    retainedBytes -= item.bytes;
    // One deletion per fragment at message completion; admission never scans.
    for (const tsn of item.tsns) fragmentsByTsn.delete(tsn);
    messages!.delete(justDeliveredSeq);
    if (messages!.size === 0) tracked.delete(streamId);
  });
  const resetSub = association.onReconfigStreams.subscribe((streamIds) => {
    if (blocked) return;
    for (const streamId of streamIds) forgetStream(streamId);
  });

  association.receiveDataChunk = function guardedReceive(chunk: DataChunk): void {
    if (blocked) return;
    const size = chunk?.userData?.byteLength;
    if (!Number.isSafeInteger(size) || size < 0 ||
        // A complete zero-byte message is valid; a zero-byte fragment is not.
        (size === 0 && (chunk.flags & 0x03) !== 0x03) ||
        !Number.isSafeInteger(chunk?.tsn) || chunk.tsn < 0 || chunk.tsn > 0xffffffff ||
        !Number.isSafeInteger(chunk?.streamId) || chunk.streamId < 0 || chunk.streamId > 65535 ||
        !Number.isSafeInteger(chunk?.streamSeqNum) || chunk.streamSeqNum < 0 ||
        chunk.streamSeqNum > 65535 || (chunk.flags & DATA_UNORDERED) !== 0) {
      violation();
      return;
    }
    // Enforce the association-wide memory window BEFORE Werift marks the
    // chunk received. Existing reassembly chunks already occupy the window.
    // Before admission, follow Werift's TSN duplicate predicate exactly.
    // A legitimate retransmission can arrive with rwnd=0; it must still
    // reach the native SACK handler without consuming any receive credit.
    const last = association.lastReceivedTsn;
    const duplicated = (last !== undefined &&
      (last === chunk.tsn ||
        (last < chunk.tsn && chunk.tsn - last > 0x80000000) ||
        (last > chunk.tsn && last - chunk.tsn < 0x80000000))) ||
      association.sackMisOrdered.has(chunk.tsn);
    if (duplicated) {
      try { original.call(association, chunk); } catch { violation(); }
      return;
    }
    const fragment: Fragment = {
      streamId: chunk.streamId,
      streamSeqNum: chunk.streamSeqNum,
      flags: chunk.flags,
    };
    const before = fragmentsByTsn.get((chunk.tsn - 1) >>> 0);
    const after = fragmentsByTsn.get((chunk.tsn + 1) >>> 0);
    if ((before && !validBoundary(before, fragment)) ||
        (after && !validBoundary(fragment, after))) {
      // In Werift, adjacent TSNs would be reassembled as the SAME message.
      // Deny inconsistent flags/SSNs before the chunk enters its buffer.
      violation();
      return;
    }
    if (size > association.advertisedRwnd || size > maxRetainedBytes - retainedBytes) {
      violation();
      return;
    }
    let messages = tracked.get(chunk.streamId);
    if (!messages) {
      if (tracked.size >= MAX_TRACKED_STREAMS) {
        violation();
        return;
      }
      messages = new Map<number, TrackedMessage>();
      tracked.set(chunk.streamId, messages);
    }
    let item = messages.get(chunk.streamSeqNum);
    if (!item) {
      item = { bytes: 0, fragments: 0, tsns: new Set<number>() };
      messages.set(chunk.streamSeqNum, item);
    }
    if (size > maxMessageBytes - item.bytes ||
        item.fragments >= MAX_PARTS_PER_MESSAGE ||
        retainedFragments >= MAX_TRACKED_FRAGMENTS) {
      violation();
      return;
    }
    item.bytes += size;
    item.fragments++;
    item.tsns.add(chunk.tsn);
    fragmentsByTsn.set(chunk.tsn, fragment);
    retainedBytes += size;
    retainedFragments++;
    try {
      original.call(association, chunk);
    } catch {
      violation();
    }
  };

  return () => {
    if (cleaned) return;
    cleaned = true;
    blocked = true;
    association.receiveDataChunk = original;
    messageSub.unSubscribe();
    resetSub.unSubscribe();
    tracked.clear();
    fragmentsByTsn.clear();
  };
}
