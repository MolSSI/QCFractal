// Checks that no route of the web portal starts with a path prefix reserved for the QCFractal server.
// When served by QCFractal (or behind a reverse proxy), requests under these prefixes go to the
// server, so a portal route there would break when reloaded or linked to.

import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import ts from "typescript";

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

// Find every path attribute in the routing. Each must be a plain string, so that it can be checked here
const appSource = readFileSync(appPath, "utf8");
const sourceFile = ts.createSourceFile(
  appPath,
  appSource,
  ts.ScriptTarget.Latest,
  true,
  ts.ScriptKind.TSX,
);
const routePaths = [];

function visit(node) {
  // A spread could hide a path attribute
  if (
    ts.isJsxSpreadAttribute(node) &&
    node.parent.parent.tagName.getText(sourceFile) === "Route"
  ) {
    const { line } = sourceFile.getLineAndCharacterOfPosition(
      node.getStart(sourceFile),
    );
    fail(
      `Route at ${appPath}:${line + 1} uses spread attributes, so its path can't be checked`,
    );
  }

  if (ts.isJsxAttribute(node) && node.name.getText(sourceFile) === "path") {
    let value = node.initializer;
    if (value && ts.isJsxExpression(value)) {
      value = value.expression;
    }

    // Strings with html entities (&...;) are decoded at runtime, so can't be checked as written
    const isPlainString =
      value &&
      (ts.isStringLiteral(value) ||
        ts.isNoSubstitutionTemplateLiteral(value)) &&
      !value.text.includes("&");
    if (!isPlainString) {
      const { line } = sourceFile.getLineAndCharacterOfPosition(
        node.getStart(sourceFile),
      );
      fail(
        `Route path at ${appPath}:${line + 1} is not a plain string literal, so it can't be checked`,
      );
    }
    routePaths.push(value.text);
  }
  ts.forEachChild(node, visit);
}
visit(sourceFile);

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
