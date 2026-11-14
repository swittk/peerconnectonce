/**
 * Sketch of host integration, without importing any framework-specific authentication or service.
 * The /signal package exports the handler; the HTTP route is /peer.
 * Mount it using the host's Request/Response conversion without stripping
 * the original URL pathname. The library never opens a LAN proxy.
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

async function demonstrateBrowser(): Promise<void> {
  // Browser-side entrypoint: in an application, place this in a separate browser
  // bundle. The browser bundle must import only /browser, never /server.
  const browser = new BrowserPeerClient({
    signaling: createHttpBrowserSignaling({
      baseUrl: 'https://node.example.test/peer',
      headers: () => YOUR_BROWSER_AUTH_HEADERS,
    }),
  });
  try {
    const opened = await browser.connect('echo-v1', { nodeId: 'node-a' });
    const reply = await new Promise<string>((resolve) => {
      opened.channel.addEventListener('message', (event) => resolve(String(event.data)), { once: true });
      opened.channel.send('hello');
    });
    console.log(reply);
  } finally {
    browser.close();
  }
}

// The host controls application lifetime and stops the node when finished.
async function demonstrateLifecycle(): Promise<void> {
  // Install the Request/Response handler at /peer; the application must keep
  // the full pathname when adapting incoming framework-specific HTTP requests.
  MOUNT_PEER_HANDLER('/peer', handler);

  const shutdown = new AbortController();
  // Start claims concurrently; awaiting start() here would block the browser.
  const running = peerServer.start(shutdown.signal);
  try {
    await demonstrateBrowser();
  } finally {
    shutdown.abort();
    await running;
  }
}

// Placeholder symbols intentionally belong to the embedding application.
declare function MOUNT_PEER_HANDLER(path: string, handler: (request: Request) => Promise<Response>): void;
declare function YOUR_HOST_AUTHENTICATION(request: Request): Promise<{ id: string } | undefined>;
declare function YOUR_NODE_AUTHENTICATION(request: Request): Promise<{ nodeId: string } | undefined>;
declare const YOUR_ICE_SERVERS: RTCIceServer[];
declare const YOUR_BROWSER_AUTH_HEADERS: HeadersInit;
void demonstrateLifecycle;
