/**
 * Sketch of host integration, without importing any framework-specific authentication or service.
 * /signal can be mounted using standard Request/Response conversion;
 * this helper library never opens an unrestricted local HTTP proxy.
 */
import { createPeerSignalHandler } from '@peerconnectonce/core/signal';
import { PeerServer, createHttpNodeSignaling } from '@peerconnectonce/core/server';
import { BrowserPeerClient, createHttpBrowserSignaling } from '@peerconnectonce/core/browser';

const handler = createPeerSignalHandler({
  authenticateClient: async (request) => {
    const user = await YOUR_HOST_AUTHENTICATION(request);
    return user ? { userId: user.id } : undefined;
  },
  authenticateNode: async (request) => YOUR_NODE_AUTHENTICATION(request),
  allowedProtocols: ['echo-v1'],
  getIceServers: async () => YOUR_ICE_SERVERS,
});

const peerServer = new PeerServer({
  signaling: createHttpNodeSignaling({
    baseUrl: 'https://node.example.test/peer',
    nodeId: 'node-a',
    headers: () => ({ authorization: 'Bearer <server-only-node-token>' }),
  }),
  services: [{ label: 'echo-v1', create: ({ writer }) => ({
    receiveMessage: (message) => writer.sendText(String(message)),
    close: () => writer.close(),
  }) }],
  iceServers: [],
});

// Browser separate entrypoint:
const browser = new BrowserPeerClient({
  signaling: createHttpBrowserSignaling({
    baseUrl: 'https://node.example.test/peer',
    headers: () => YOUR_BROWSER_AUTH_HEADERS,
  }),
});
const opened = await browser.connect('echo-v1', { nodeId: 'node-a' });
opened.channel.send('hello');

// Placeholder symbols intentionally belong to each embedding application.
declare function YOUR_HOST_AUTHENTICATION(request: Request): Promise<{ id: string } | undefined>;
declare function YOUR_NODE_AUTHENTICATION(request: Request): Promise<{ nodeId: string } | undefined>;
declare const YOUR_ICE_SERVERS: RTCIceServer[];
declare const YOUR_BROWSER_AUTH_HEADERS: HeadersInit;
void handler; void peerServer;
