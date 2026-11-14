import { execFileSync } from 'node:child_process';

const tsc = new URL('../node_modules/typescript/bin/tsc', import.meta.url).pathname;
const files = [
  new URL('../tests/consumer-node-only.mts', import.meta.url).pathname,
  new URL('../tests/consumer-node-only.cts', import.meta.url).pathname,
];
execFileSync(process.execPath, [
  tsc,
  '--module', 'NodeNext', '--moduleResolution', 'NodeNext',
  '--target', 'ES2018', '--lib', 'ES2018', '--types', 'node',
  '--skipLibCheck', 'false', '--strict', '--noEmit',
  ...files,
], { stdio: 'inherit' });
console.log('PASS Node-only ES2018 ESM+CJS declarations without DOM or skipLibCheck');
