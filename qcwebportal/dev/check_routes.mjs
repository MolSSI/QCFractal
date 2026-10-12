// Checks that no route of the web portal starts with a path prefix reserved for the QCFractal server.
// When served by QCFractal (or behind a reverse proxy), requests under these prefixes go to the
// server, so a portal route there would break when reloaded or linked to.

import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const myDir = dirname(fileURLToPath(import.meta.url));
const homePath = join(
  myDir,
  "..",
  "..",
  "qcfractal",
  "qcfractal",
  "flask_app",
  "home_v1.py",
);
const appPath = join(myDir, "..", "src", "App.tsx");

function fail(msg) {
  console.error(msg);
  process.exit(1);
}

// The reserved prefixes are defined by the server
const homeSource = readFileSync(homePath, "utf8");
const prefixMatch = homeSource.match(
  /^RESERVED_PATH_PREFIXES\s*=\s*\(([^)]*)\)/m,
);
if (!prefixMatch) {
  fail(`Could not find RESERVED_PATH_PREFIXES in ${homePath}`);
}
const reservedPrefixes = [...prefixMatch[1].matchAll(/["']([^"']+)["']/g)].map(
  (m) => m[1],
);
if (reservedPrefixes.length === 0) {
  fail(`RESERVED_PATH_PREFIXES in ${homePath} is empty`);
}

// Every route path must be a string literal, so that it can be checked here
const appSource = readFileSync(appPath, "utf8");
const pathAttrs = [...appSource.matchAll(/\bpath=(\S+)/g)].map((m) => m[1]);
const routePaths = [];
for (const attr of pathAttrs) {
  const literal = attr.match(
    /^(?:"([^"]*)"|'([^']*)'|\{["'`]([^"'`$]*)["'`]\})/,
  );
  if (!literal) {
    fail(
      `Route path ${attr} in ${appPath} is not a string literal, so it can't be checked`,
    );
  }
  routePaths.push(literal[1] ?? literal[2] ?? literal[3]);
}

if (routePaths.length === 0) {
  fail(`No routes found in ${appPath}. Has the routing moved?`);
}

const bad = routePaths.filter((p) =>
  reservedPrefixes.includes(p.replace(/^\/+/, "").split("/")[0]),
);
if (bad.length > 0) {
  fail(
    `Routes use a path prefix reserved for the server (${reservedPrefixes.join(", ")}):\n` +
      bad.map((p) => `  ${p}`).join("\n"),
  );
}

console.log(
  `Checked ${routePaths.length} routes against reserved prefixes ${reservedPrefixes.join(", ")}`,
);
