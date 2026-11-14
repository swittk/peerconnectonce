import {
  PeerClient, createHttpClientSignaling,
  type PeerClientOptions, type PeerConnectionFactory,
  type ClientPeerConnection,
} from '../client/index.js';
export { createHttpClientSignaling as createHttpBrowserSignaling };
export type { PeerClientSignaling, PeerSessionCreation, PeerSessionStatus } from '../client/index.js';

/** Native-browser adapter. The shared /client implementation never creates WebRTC objects. */
export class BrowserPeerClient extends PeerClient {
  constructor(options: Omit<PeerClientOptions, 'createPeerConnection'> & {
    createPeerConnection?: PeerConnectionFactory;
  }) {
    super({
      ...options,
      createPeerConnection: options.createPeerConnection ??
        ((configuration) => new RTCPeerConnection(configuration) as unknown as ClientPeerConnection),
    });
  }
}
