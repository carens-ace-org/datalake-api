const fs = require('node:fs');
const path = require('node:path');
const { spawnSync } = require('node:child_process');

const root = path.resolve(__dirname, '..');
const excludedDirectories = new Set([
  '.git',
  '.metadata',
  'TEST_SERVER',
  'BARfiles',
  'GeneratedBarFiles',
]);

function findDescriptors(directory, found = []) {
  for (const entry of fs.readdirSync(directory, { withFileTypes: true })) {
    if (entry.isDirectory() && excludedDirectories.has(entry.name)) continue;

    const fullPath = path.join(directory, entry.name);
    if (entry.isDirectory()) {
      findDescriptors(fullPath, found);
    } else if (entry.isFile() && entry.name === 'restapi.descriptor') {
      found.push(fullPath);
    }
  }
  return found;
}

const specs = new Set();
for (const descriptorPath of findDescriptors(root)) {
  const descriptor = fs.readFileSync(descriptorPath, 'utf8');
  const match = descriptor.match(/\bdefinitionFile="([^"]+)"/);
  if (!match) {
    console.error(`No definitionFile found in ${path.relative(root, descriptorPath)}`);
    process.exitCode = 1;
    continue;
  }

  const specPath = path.resolve(path.dirname(descriptorPath), match[1]);
  if (!fs.existsSync(specPath)) {
    console.error(`OpenAPI file not found: ${path.relative(root, specPath)}`);
    process.exitCode = 1;
    continue;
  }
  specs.add(specPath);
}

if (specs.size === 0) {
  console.error('No OpenAPI specs were found through ACE REST API descriptors.');
  process.exit(1);
}

const args = [
  '--yes',
  '@redocly/cli@1.34.5',
  'lint',
  '--config',
  path.join(root, 'redocly.yaml'),
  ...[...specs].sort(),
];

const command = process.platform === 'win32' ? 'npx.cmd' : 'npx';
const commandLine = [command, ...args]
  .map((argument) => `"${argument.replaceAll('"', '\\"')}"`)
  .join(' ');
const result = spawnSync(commandLine, {
  cwd: root,
  stdio: 'inherit',
  shell: true,
});

if (result.error) {
  console.error(`Unable to start Redocly CLI: ${result.error.message}`);
  process.exit(1);
}
process.exit(result.status ?? 1);
