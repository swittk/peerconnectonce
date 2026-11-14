import { execFileSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';

const tsc = fileURLToPath(new URL('../node_modules/typescript/bin/tsc', import.meta.url));
const files = [
  fileURLToPath(new URL('../tests/consumer-node-only.mts', import.meta.url)),
  fileURLToPath(new URL('../tests/consumer-node-only.cts', import.meta.url)),
];
execFileSync(process.execPath, [
  tsc,
  '--module', 'NodeNext', '--moduleResolution', 'NodeNext',
  '--target', 'ES2018', '--lib', 'ES2018', '--types', 'node',
  '--skipLibCheck', 'false', '--strict', '--noEmit',
  ...files,
], { stdio: 'inherit' });
console.log('PASS Node-only ES2018 ESM+CJS declarations without DOM or skipLibCheck');
