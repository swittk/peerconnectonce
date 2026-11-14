import type { PeerHttpHeaders } from './contracts.js';

/**
 * Resolve host-provided authorization headers without sending a request after
 * cancellation. Shared by the browser/client and Node signaling adapters.
 * Credential refreshes may ignore AbortSignal, so the wrapper must settle
 * independently and fence the later transport call.
 */
export function resolvePeerHeaders(
  readHeaders: () => PeerHttpHeaders | Promise<PeerHttpHeaders>,
  signal?: AbortSignal | null,
): Promise<Headers> {
  if (signal?.aborted) return Promise.reject(new Error('peer_request_aborted'));
  return new Promise<Headers>((resolve, reject) => {
    let settled = false;
    const cleanup = () => signal?.removeEventListener('abort', onAbort);
    const finish = (error?: Error, value?: Headers) => {
      if (settled) return;
      settled = true;
      cleanup();
      if (error) reject(error);
      else resolve(value!);
    };
    const onAbort = () => finish(new Error('peer_request_aborted'));
    signal?.addEventListener('abort', onAbort, { once: true });
    Promise.resolve().then(() => {
      if (signal?.aborted) throw new Error('peer_request_aborted');
      return readHeaders();
    }).then((values) => {
      if (signal?.aborted) return finish(new Error('peer_request_aborted'));
      try {
        const headers = new Headers(values);
        if (signal?.aborted) return finish(new Error('peer_request_aborted'));
        finish(undefined, headers);
      } catch (error) {
        finish(error instanceof Error ? error : new Error('invalid_peer_headers'));
      }
    }, (error: unknown) => {
      finish(error instanceof Error ? error : new Error('peer_headers_failed'));
    });
  });
}
