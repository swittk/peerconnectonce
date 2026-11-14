import { createServer } from 'node:http';
import { readFile } from 'node:fs/promises';
import { createRequire } from 'node:module';
import { fileURLToPath } from 'node:url';
import { resolve, dirname } from 'node:path';
import { createPeerSignalHandler, MemoryPeerSessionStore } from '../dist/signal/index.js';
import { createHttpNodeSignaling, PeerServer } from '../dist/server/index.js';

const root = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const require = createRequire(import.meta.url);
let chromium;
try {
  // Optional test dependency: browser installation does not belong in the package runtime.
  chromium = require(process.env.PEERCONNECTONCE_PLAYWRIGHT ?? 'playwright').chromium;
} catch {
  throw new Error('Install playwright for this browser smoke, or set PEERCONNECTONCE_PLAYWRIGHT to its absolute module path');
}

const signalHandler = createPeerSignalHandler({
  basePath: '/peer',
  authenticateClient: async (request) =>
    request.headers.get('x-test-user') === 'alice' ? { userId: 'alice' } : undefined,
  authenticateNode: async (request) =>
    request.headers.get('authorization') === 'Bearer node-token' ? { nodeId: 'node-a' } : undefined,
  allowedProtocols: ['echo-v1'],
  resolveNode: async () => 'node-a',
  getIceServers: async () => [],
  // A second browser session must depend on the first browser explicitly
  // acknowledging its answer; otherwise the first is protected from eviction.
  store: new MemoryPeerSessionStore({ maxSessions: 1, maxSessionsPerUser: 1, maxPendingPerUser: 1 }),
});

const server = createServer(async (request, response) => {
  try {
    const pathname = new URL(request.url ?? '/', 'http://localhost').pathname;
    if (pathname === '/' || pathname === '/index.html') {
      response.setHeader('content-type', 'text/html');
      response.end('<!doctype html><title>PeerConnectOnce browser smoke</title>');
      return;
    }
    if (pathname === '/dist/browser/index.js' || pathname === '/dist/client/index.js' ||
        pathname === '/dist/shared/contracts.js' || pathname === '/dist/shared/http.js') {
      const path = resolve(root, pathname.slice(1));
      response.setHeader('content-type', 'text/javascript');
      response.end(await readFile(path));
      return;
    }
    if (!pathname.startsWith('/peer/')) {
      response.statusCode = 404;
      response.end();
      return;
    }
    const chunks = [];
    for await (const chunk of request) chunks.push(chunk);
    const headers = new Headers();
    for (const [name, value] of Object.entries(request.headers)) {
      if (value !== undefined) headers.set(name, Array.isArray(value) ? value.join(',') : value);
    }
    const input = new Request('http://127.0.0.1:' + server.address().port + request.url, {
      method: request.method, headers,
      ...(request.method === 'POST' ? { body: Buffer.concat(chunks) } : {}),
    });
    const result = await signalHandler(input);
    response.writeHead(result.status, Object.fromEntries(result.headers));
    response.end(Buffer.from(await result.arrayBuffer()));
  } catch (error) {
    response.statusCode = 500;
    response.end('internal_error');
  }
});

const shutdown = new AbortController();
const receivedMessages = [];
let browser;
try {
  await new Promise((resolve) => server.listen(0, '127.0.0.1', resolve));
  const origin = 'http://127.0.0.1:' + server.address().port;
  const node = new PeerServer({
    signaling: createHttpNodeSignaling({
      baseUrl: origin + '/peer',
      nodeId: 'node-a',
      headers: () => ({ authorization: 'Bearer node-token' }),
    }),
    services: [{
      label: 'echo-v1',
      create: ({ writer, beginWork }) => ({
        async receiveMessage(message) {
          const release = beginWork();
          try {
            const text = String(message);
            receivedMessages.push(text);
            if (text === 'fail-first') {
              // The next wire message must be queued before this failure closes the session.
              await new Promise((resolve) => setTimeout(resolve, 150));
              throw new Error('intentional_adapter_failure');
            }
            // A long-running registered service must outlive an idle timeout.
            if (text === 'slow') {
              await new Promise((resolve) => setTimeout(resolve, 3500));
            }
            await writer.sendText('echo:' + text);
          } finally {
            release();
          }
        },
        close() {},
      }),
    }],
    iceServers: [], pollIntervalMs: 15, maxPeers: 4, idleTimeoutMs: 2000,
  });
  const running = node.start(shutdown.signal);
  browser = await chromium.launch({ headless: true, args: ['--no-sandbox'] });
  const page = await browser.newPage();
  await page.goto(origin + '/');
  const result = await page.evaluate(async (base) => {
    const { BrowserPeerClient, createHttpBrowserSignaling } = await import('/dist/browser/index.js');
    const client = new BrowserPeerClient({
      signaling: createHttpBrowserSignaling({
        baseUrl: base + '/peer',
        headers: () => ({ 'x-test-user': 'alice' }),
      }),
      connectionTimeoutMs: 20_000,
    });
    const opened = await client.connect('echo-v1');
    const message = await new Promise((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error('echo_response_timeout')), 10_000);
      opened.channel.addEventListener('message', (event) => {
        clearTimeout(timeout);
        resolve(event.data);
      }, { once: true });
      opened.channel.send('ping');
    });
    // A 32 KiB ordered message traverses many SCTP DATA fragments. The
    // receive guard must allow complete legitimate reassembly, not merely
    // small, single-packet signaling messages.
    const largePayload = '0123456789abcdef'.repeat(2048);
    const largeEcho = await new Promise((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error('fragmented_echo_timeout')), 10_000);
      opened.channel.addEventListener('message', (event) => {
        clearTimeout(timeout);
        resolve(event.data);
      }, { once: true });
      opened.channel.send(largePayload);
    });
    if (largeEcho !== 'echo:' + largePayload) throw new Error('fragmented_echo_mismatch');
    const slow = await new Promise((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error('active_stream_did_not_survive_idle')), 7000);
      opened.channel.addEventListener('message', (event) => {
        clearTimeout(timeout);
        resolve(event.data);
      }, { once: true });
      opened.channel.send('slow');
    });
    const state = opened.channel.readyState;
    const idleClosed = await new Promise((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error('idle_connection_not_cleaned_up')), 6000);
      opened.channel.addEventListener('close', () => {
        clearTimeout(timeout);
        resolve(true);
      }, { once: true });
    });
    client.close();
    return { message, fragmentedBytes: largePayload.length, slow, state, idleClosed, closed: opened.channel.readyState };
  }, origin);
  if (result.message !== 'echo:ping' || result.slow !== 'echo:slow' ||
      result.state !== 'open' || !result.idleClosed || result.closed !== 'closed') {
    throw new Error('unexpected browser/werift echo: ' + JSON.stringify(result));
  }
  // The ordered transport must not process queued service B after handler A
  // failed and closed the session, even if both frames arrived before close.
  const failedSessionClosed = await page.evaluate(async (base) => {
    const { BrowserPeerClient, createHttpBrowserSignaling } = await import('/dist/browser/index.js');
    const client = new BrowserPeerClient({
      signaling: createHttpBrowserSignaling({
        baseUrl: base + '/peer',
        headers: () => ({ 'x-test-user': 'alice' }),
      }),
      connectionTimeoutMs: 20_000,
    });
    const opened = await client.connect('echo-v1');
    const closed = new Promise((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error('failed_adapter_did_not_close')), 5_000);
      opened.channel.addEventListener('close', () => {
        clearTimeout(timer);
        resolve(true);
      }, { once: true });
    });
    opened.channel.send('fail-first');
    opened.channel.send('must-not-process');
    const didClose = await closed;
    client.close();
    return didClose;
  }, origin);
  if (!failedSessionClosed || receivedMessages.includes('must-not-process') ||
      !receivedMessages.includes('fail-first')) {
    throw new Error('queued service was invoked after peer close: ' + JSON.stringify(receivedMessages));
  }
  console.log('PEERCONNECTONCE_BROWSER_WERIFT_PASS', JSON.stringify({
    ...result, queuedMessagesSkippedAfterFailure: true,
  }));
  shutdown.abort();
  await running;
} finally {
  shutdown.abort();
  await browser?.close();
  await new Promise((resolve) => server.close(resolve));
}
