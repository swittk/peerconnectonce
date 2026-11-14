/** Contracts shared across clients, signaling services, and peer runtimes. */
export const PEER_PROTOCOL_LABEL = /^[a-z][a-z0-9-]{0,63}$/;
export const MAX_SDP_BYTES = 256 * 1024;
export const MAX_PEER_MESSAGE_BYTES = 64 * 1024;
export const DEFAULT_NODE_ID = 'default';

export type PeerIceServer = {
  urls: string | string[];
  username?: string;
  credential?: string;
};
export type PeerOffer = {
  sessionId: string;
  nodeId: string;
  ownerUserId: string;
  protocol: string;
  offerSdp: string;
};
export type PeerSessionStatus =
  | { status: 'pending' }
  | { status: 'answered'; answerSdp: string }
  | { status: 'failed' };
export type PeerSessionCreation = { sessionId: string; iceServers: PeerIceServer[] };
export type PeerIceConfiguration = { iceServers: PeerIceServer[] };

export interface PeerClientSignaling {
  getIceServers(options: { protocol: string; nodeId?: string; signal?: AbortSignal }): Promise<PeerIceServer[]>;
  createOffer(args: { protocol: string; offerSdp: string; nodeId?: string; signal?: AbortSignal }): Promise<PeerSessionCreation>;
  readAnswer(sessionId: string, signal?: AbortSignal): Promise<PeerSessionStatus>;
  cancelOffer?(sessionId: string, signal?: AbortSignal): Promise<void>;
}

export interface PeerNodeSignaling {
  claim(signal?: AbortSignal): Promise<PeerOffer | undefined>;
  answer(sessionId: string, answerSdp: string, signal?: AbortSignal): Promise<void>;
  fail(sessionId: string, signal?: AbortSignal): Promise<void>;
}

export function isPeerLabel(value: string): boolean {
  return PEER_PROTOCOL_LABEL.test(value);
}

/** Accept platform-neutral ICE configuration without forcing any default STUN provider. */
export function readPeerIceServers(value: unknown): PeerIceServer[] {
  if (!Array.isArray(value) || value.length > 16) throw new Error('invalid_peer_ice_config');
  const result: PeerIceServer[] = [];
  for (const entry of value) {
    if (typeof entry !== 'object' || entry === null || Array.isArray(entry)) throw new Error('invalid_peer_ice_config');
    const candidate = entry as Record<string, unknown>;
    const urls = candidate.urls;
    if (!(typeof urls === 'string' || (Array.isArray(urls) && urls.length > 0 && urls.every((url) => typeof url === 'string'))))
      throw new Error('invalid_peer_ice_config');
    if (candidate.username !== undefined && typeof candidate.username !== 'string') throw new Error('invalid_peer_ice_config');
    if (candidate.credential !== undefined && typeof candidate.credential !== 'string') throw new Error('invalid_peer_ice_config');
    result.push({
      urls: urls as string | string[],
      ...(candidate.username === undefined ? {} : { username: candidate.username as string }),
      ...(candidate.credential === undefined ? {} : { credential: candidate.credential as string }),
    });
  }
  return result;
}
