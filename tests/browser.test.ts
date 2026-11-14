import { describe, expect, it } from 'vitest';
import { BrowserPeerClient } from '../src/browser/index.js';
import { PeerClient } from '../src/client/index.js';

describe('browser adapter', () => {
  it('reuses the platform-neutral client implementation without its own state machine', () => {
    const client = new BrowserPeerClient({
      signaling: {
        getIceServers: async () => [],
        createOffer: async () => ({ sessionId: 'one', iceServers: [] }),
        readAnswer: async () => ({ status: 'pending' }),
      },
      createPeerConnection: () => { throw new Error('test-only-driver'); },
    });
    expect(client).toBeInstanceOf(PeerClient);
    client.close();
  });
});
