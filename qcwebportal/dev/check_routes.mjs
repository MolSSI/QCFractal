// Checks that no route of the web portal starts with a path prefix reserved for the QCFractal server.
// When served by QCFractal (or behind a reverse proxy), requests under these prefixes go to the
// server, so a portal route there would break when reloaded or linked to.

import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

// Must match RESERVED_PATH_PREFIXES in qcfractal/qcfractal/flask_app/home_v1.py
const reservedPrefixes = ["api", "auth", "compute"];

const appPath = join(
  dirname(fileURLToPath(import.meta.url)),
  "..",
  "src",
  "App.tsx",
);
const appSource = readFileSync(appPath, "utf8");

const routePaths = [...appSource.matchAll(/\bpath=["'{`]+([^"'}`]+)/g)].map(
  (m) => m[1],
);

if (routePaths.length === 0) {
  console.error(`No routes found in ${appPath}. Has the routing moved?`);
  process.exit(1);
}

const bad = routePaths.filter((p) =>
  reservedPrefixes.includes(p.replace(/^\/+/, "").split("/")[0]),
);

if (bad.length > 0) {
  console.error(
    `Routes use a path prefix reserved for the server (${reservedPrefixes.join(", ")}):`,
  );
  bad.forEach((p) => console.error(`  ${p}`));
  process.exit(1);
}

console.log(`Checked ${routePaths.length} routes`);
