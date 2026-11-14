import { readFileSync } from 'node:fs';
import { createRequire } from 'node:module';
const pkg = JSON.parse(readFileSync(new URL('../package.json', import.meta.url), 'utf8'));
if (pkg.name !== '@peerconnectonce/core') throw new Error('incorrect package ownership');
if (Object.keys(pkg.exports).join(',') !== './client,./browser,./server,./signal') throw new Error('unexpected entrypoints');
const require = createRequire(import.meta.url);
for (const entry of ['client', 'browser', 'server', 'signal']) {
  const importApi = await import('@peerconnectonce/core/' + entry);
  const requireApi = require('@peerconnectonce/core/' + entry);
  if (Object.keys(importApi).length === 0 || Object.keys(requireApi).length === 0) {
    throw new Error('empty export: ' + entry);
  }
}
console.log('PASS distinct client/browser/server/signal ESM + CJS exports');
