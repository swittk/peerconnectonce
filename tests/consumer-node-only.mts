/**
 * Type-only consumer proof: Node + ES2018, intentionally WITHOUT lib.dom and
 * without skipLibCheck. Both ESM and CJS conditional typings must resolve.
 */
import {
  PeerServer, PeerServiceRegistry, BoundedPeerWriter, type PeerServerOptions,
  createHttpNodeSignaling,
} from '@peerconnectonce/core/server';
import { createPeerSignalHandler } from '@peerconnectonce/core/signal';

const config: PeerServerOptions = {
  signaling: {
    claim: async () => undefined,
    answer: async () => {},
    fail: async () => {},
  },
  services: [{ label: 'echo-v1', create: () => ({
    receiveMessage() {},
    close() {},
  }) }],
  iceServers: [],
};
void [PeerServer, PeerServiceRegistry, BoundedPeerWriter, config,
  createHttpNodeSignaling, createPeerSignalHandler];
