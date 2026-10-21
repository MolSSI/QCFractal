# QCFractal Web Portal

A web interface for a QCFractal server, for browsing projects, datasets, records, and managers, and for
administering users and the server. It is a React + TypeScript single-page application built with
[Vite](https://vite.dev/), and talks to the QCFractal server's REST API.

The web portal is still under active development, and may be released separately from (and more often than)
the QCFractal server.

## Building

Requires Node.js 22.12 or newer.

```shell
npm ci
npm run build
```

The result is a static site in `dist/`, which can be served by any web server (see [Deployment](#deployment)).

### Server address

By default, the portal expects the QCFractal server to be on the same origin (scheme, host, and port) as the
portal itself. That is, if the portal is at `https://qcarchive.example.com/`, it uses
`https://qcarchive.example.com/api/v1/...`. This means one build can be used with any server, and is the
recommended setup.

To use a server at a different address instead, set `VITE_QCFRACTAL_URI` when building. This can be an
environment variable or a line in a `.env.production` or `.env.local` file:

```shell
VITE_QCFRACTAL_URI=https://api.example.com npm run build
```

The address is baked into the built files; changing it requires rebuilding. A server at a different origin
must also be configured to allow the portal (see [Separate origin](#separate-origin)).

## Deployment

The portal must be served from the root of a host (`https://host/`, not `https://host/portal/`). Requests
for paths that are not files (such as `/projects/123`) must be answered with `index.html`, since those are
routes handled by the portal itself.

The QCFractal server owns all paths under `/api/`, `/auth/`, and `/compute/`. The portal never uses those
paths for its own routes (this is checked in CI by `dev/check_routes.mjs`).

### Served by the QCFractal server

The simplest setup. Point the server's `homepage_directory` setting at the built files:

```yaml
homepage_directory: /path/to/qcwebportal/dist
```

The server then serves the portal at its root, alongside the API.

### Behind a reverse proxy

The portal can be served by a separate web server (allowing it to be updated independently of the
QCFractal server), with requests under `/api/` and `/auth/` forwarded to the QCFractal server. To the browser,
everything is on the same origin, so no further configuration is needed.

The proxy must pass along the original `Host` header, including the port (for nginx,
`proxy_set_header Host $http_host;`, since `$host` drops the port). The server compares it with the browser's `Origin` header to reject cross-site requests, so logins and other changes
fail with a 403 error if the proxy replaces it with the address of the QCFractal server.

The `Dockerfile` builds an image that serves the portal with nginx (it does not include the proxy
configuration):

```shell
docker build -t qcwebportal .
docker build --build-arg VITE_QCFRACTAL_URI=https://api.example.com -t qcwebportal .
```

### Separate origin

The portal can be on a different origin than the server. Build the portal with `VITE_QCFRACTAL_URI` set to
the server's address, and allow the portal's origin in the server configuration. The server's defaults do not
allow any of this, so the server configuration must be changed as below.

The portal logs in with a session cookie, which works differently depending on whether the portal and server
are on the same _site_ (the same registrable domain, such as `example.com`).

**Same site, different origin** (such as `https://portal.example.com` and `https://api.example.com`). The
browser treats the session cookie as a first-party cookie, so only CORS needs to be enabled:

```yaml
cors:
  enabled: true
  origins:
    - https://portal.example.com
  supports_credentials: true
```

**Different sites** (such as `https://portal.example.org` and `https://qcfractal.example.com`). In addition to
the CORS settings above, the session cookie must be allowed to be sent cross-site:

```yaml
cors:
  enabled: true
  origins:
    - https://portal.example.org
  supports_credentials: true

api:
  user_session_cookie_samesite: "None"
  user_session_cookie_secure: true # required with SameSite=None (the default)
  user_session_cookie_partitioned: true
```

The server must be served over HTTPS. Even then, the session cookie is now a third-party cookie, which some
browsers block (Safari by default, others depending on user settings or private browsing). Users of those
browsers cannot log in; the portal tells them their browser did not keep the login cookie. Browsing data
anonymously (if the server allows it) still works. Prefer a same-origin or same-site setup if possible.

The `origins` must exactly match the portal's origin (scheme, host, and port if not the default), with no
trailing slash. Requests from origins not listed are rejected by the server's cross-site request checks.

## Development

```shell
npm ci
npm run dev
```

This starts a development server (by default at `http://localhost:5173`) that reloads as files change.
Point it at a QCFractal server with `VITE_QCFRACTAL_URI`, either on the command line or in `.env.local`:

```shell
VITE_QCFRACTAL_URI=https://qcfractal.example.com npm run dev
```

The development server proxies `/api/` and `/auth/` requests to that server, so to the browser the server is on
the same origin as the portal, and no server configuration is needed. This works with local and remote servers.

The proxy makes requests from the portal look like they come from the server's own origin, so that the server's
cross-site request checks accept them. Requests from other origins (such as other sites open in the same browser)
are passed through unchanged, and so are still rejected by the server. As with any development server, don't
expose it beyond your own machine.

To have the portal talk to the server directly instead, also set `QCFRACTAL_DEV_DIRECT=1`. This requires the
server to allow the development server's origin (see [Separate origin](#separate-origin), with
`http://localhost:5173` as the origin), and is mainly useful for testing such setups.

The server's session cookie is marked `Secure` by default. Most browsers accept that on `http://localhost`, but if
you access the development server through a different hostname over plain HTTP, logins will not stick.

Other useful commands:

```shell
npm run build      # Type-check and build into dist/
npm run devbuild   # Build without type-checking
npm run preview    # Serve the contents of dist/ locally
npm run typecheck  # Type-check only
npm run lint       # Run ESLint
npm run format     # Format with Prettier
```

### Role permissions

`src/global_role_permissions.json` is generated from the server's role definitions
(`qcfractal/qcfractal/components/auth/global_role_permissions.yaml`). After changing those, regenerate it with

```shell
python dev/convert_role_permissions.py
```
