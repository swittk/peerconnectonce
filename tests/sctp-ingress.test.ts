import { describe, expect, it } from 'vitest';
import { RTCPeerConnection } from 'werift';
import { protectWeriftSctpReceive, type WeriftSctpReceiveSurface } from '../src/server/sctpIngress.js';

/**
 * Use the actual versioned Werift SCTP implementation, not a mock of our
 * own wrapper, to ensure incomplete DATA fragments are rejected before they
 * enter the internal InboundStream.reassembly list.
 */
async function withAssociation<T>(test: (association: any, peer: RTCPeerConnection) => Promise<T> | T): Promise<T> {
  const sender = new RTCPeerConnection({ iceServers: [], iceUseDefaultStunServer: false });
  const receiver = new RTCPeerConnection({ iceServers: [], iceUseDefaultStunServer: false });
  try {
    sender.createDataChannel('echo-v1', { ordered: true });
    await sender.setLocalDescription(await sender.createOffer());
    await receiver.setRemoteDescription({ type: 'offer', sdp: sender.localDescription!.sdp });
    const association = receiver.sctp?.sctp;
    if (!association) throw new Error('werift_missing_sctp_association');
    // Initial TSN is normally installed by SCTP INIT. This direct DATA
    // injection bypasses the handshake but exercises its true receive path.
    association.lastReceivedTsn = 0;
    return await test(association, receiver);
  } finally {
    await receiver.close();
    await sender.close();
  }
}
function chunk(tsn: number, size: number, streamId = 1, streamSeqNum = 0) {
  return {
    tsn, streamId, streamSeqNum,
    protocol: 53, flags: tsn === 1 ? 2 : 0,
    userData: Buffer.alloc(size),
  };
}

describe('Werift SCTP ingress bounds before reassembly', () => {
  it('rejects fragments exceeding maxMessageBytes and keeps reassembly bounded', async () => {
    await withAssociation((association, peer) => {
      let violations = 0;
      const original = association.receiveDataChunk;
      const dispose = protectWeriftSctpReceive(
        peer as unknown as WeriftSctpReceiveSurface, 4096,
        () => { violations++; },
      );
      try {
        for (let i = 1; i <= 5; i++) {
          association.receiveDataChunk(chunk(i, 1024));
        }
        expect(violations).toBe(1);
        expect(association.inboundStreams[1].reassembly).toHaveLength(4);
        const window = association.advertisedRwnd;
        association.receiveDataChunk(chunk(6, 1024));
        expect(association.advertisedRwnd).toBe(window);
        expect(association.inboundStreams[1].reassembly).toHaveLength(4);
      } finally {
        dispose();
        dispose();
        expect(association.receiveDataChunk).toBe(original);
      }
    });
  });

  it('rejects incoming data that exceeds the remaining advertised receive window', async () => {
    await withAssociation((association, peer) => {
      let violations = 0;
      association.advertisedRwnd = 2048;
      const dispose = protectWeriftSctpReceive(peer as unknown as WeriftSctpReceiveSurface, 64 * 1024,
        () => { violations++; });
      try {
        for (let i = 1; i <= 3; i++) association.receiveDataChunk(chunk(i, 1024));
        expect(violations).toBe(1);
        expect(association.advertisedRwnd).toBe(0);
        expect(association.inboundStreams[1].reassembly).toHaveLength(2);
      } finally { dispose(); }
    });
  });

  it('accepts duplicate SCTP retransmissions with a zero receive window without growing memory', async () => {
    await withAssociation((association, peer) => {
      let violations = 0;
      association.advertisedRwnd = 1024;
      const dispose = protectWeriftSctpReceive(
        peer as unknown as WeriftSctpReceiveSurface, 4096,
        () => { violations++; },
      );
      try {
        association.receiveDataChunk(chunk(1, 1024));
        expect(association.advertisedRwnd).toBe(0);
        association.receiveDataChunk(chunk(1, 1024));
        expect(violations).toBe(0);
        expect(association.inboundStreams[1].reassembly).toHaveLength(1);
        association.receiveDataChunk(chunk(2, 1024));
        expect(violations).toBe(1);
        expect(association.inboundStreams[1].reassembly).toHaveLength(1);
      } finally { dispose(); }
    });
  });

  it('keeps its own retained-byte ceiling even if Werift window credit is inflated', async () => {
    await withAssociation((association, peer) => {
      let violations = 0;
      association.advertisedRwnd = 2048;
      const dispose = protectWeriftSctpReceive(
        peer as unknown as WeriftSctpReceiveSurface, 64 * 1024,
        () => { violations++; },
      );
      try {
        for (let i = 1; i <= 3; i++) {
          // An accounting bug in the underlying SCTP implementation must not
          // allow incomplete fragments to exceed our independent 2 KiB ceiling.
          association.advertisedRwnd = 2048;
          association.receiveDataChunk(chunk(i, 1024));
        }
        expect(violations).toBe(1);
        expect(association.inboundStreams[1].reassembly).toHaveLength(2);
      } finally { dispose(); }
    });
  });

  it('does not permit zero-byte incomplete fragments or unordered channels', async () => {
    await withAssociation((association, peer) => {
      let violations = 0;
      const dispose = protectWeriftSctpReceive(peer as unknown as WeriftSctpReceiveSurface, 64 * 1024,
        () => { violations++; });
      try {
        association.receiveDataChunk(chunk(1, 0));
        expect(violations).toBe(1);
        expect(association.inboundStreams[1]).toBeUndefined();
      } finally { dispose(); }
    });
    await withAssociation((association, peer) => {
      let violations = 0;
      const dispose = protectWeriftSctpReceive(peer as unknown as WeriftSctpReceiveSurface, 64 * 1024,
        () => { violations++; });
      try {
        association.receiveDataChunk({ ...chunk(1, 1024), flags: 0x07 });
        expect(violations).toBe(1);
        expect(association.inboundStreams[1]).toBeUndefined();
      } finally { dispose(); }
    });
  });

  it('fails closed if the expected SCTP ingress surface is unavailable', () => {
    expect(() => protectWeriftSctpReceive({ sctp: null }, 1024, () => undefined))
      .toThrow('peer_sctp_ingress_unsupported');
  });
});
