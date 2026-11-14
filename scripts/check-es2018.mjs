import { readdir, readFile } from 'node:fs/promises';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { parse } from 'acorn';

/**
 * Validate emitted output independently of TypeScript's transpilation setting.
 * Both ESM and CJS must parse as ES2018; CommonJS is script, ESM is module.
 */
const manifest = JSON.parse(await readFile(new URL('../tsconfig.json', import.meta.url), 'utf8'));
if (manifest.compilerOptions?.target?.toUpperCase() !== 'ES2018') {
  throw new Error('The public JavaScript output target must remain ES2018 or lower');
}
const root = fileURLToPath(new URL('../', import.meta.url));
let files = 0;
for (const [dir, sourceType] of [['dist', 'module'], ['dist-cjs', 'script']]) {
  const pending = [join(root, dir)];
  while (pending.length) {
    const path = pending.pop();
    if (!path) break;
    for (const item of await readdir(path, { withFileTypes: true })) {
      const filepath = join(path, item.name);
      if (item.isDirectory()) {
        pending.push(filepath);
      } else if (item.name.endsWith('.js')) {
        const code = await readFile(filepath, 'utf8');
        parse(code, { ecmaVersion: 2018, sourceType });
        files++;
      }
    }
  }
}
if (files < 8) throw new Error('Missing expected ESM/CJS build outputs');
console.log('PASS all ' + files + ' emitted ESM/CJS modules parse as ES2018');
