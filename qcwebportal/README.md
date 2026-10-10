# QCFractal Web Portal

A web interface for a QCFractal server, for browsing projects, datasets, records, and managers, and for
administering users and the server. It is a React + TypeScript single-page application built with
[Vite](https://vite.dev/), and talks to the QCFractal server's REST API.

## Development

Requires Node.js 22 or newer.

```shell
npm ci
npm run dev
```

The portal needs to know where the QCFractal server is. Set this in a `.env.local` file:

```
VITE_QCFRACTAL_URI=http://localhost:7777
```

Alternatively, set `PROXY_TARGET=http://localhost:7777` and `VITE_QCFRACTAL_URI=` (empty), and the Vite dev server
will proxy `/api` and `/auth` requests to the server, avoiding cross-origin issues.

Other useful commands:

```shell
npm run build      # Type-check and build the production bundle into dist/
npm run typecheck  # Type-check only
npm run lint       # Run ESLint
npm run format     # Format with Prettier
```

## Role permissions

`src/global_role_permissions.json` is generated from the server's role definitions
(`qcfractal/qcfractal/components/auth/global_role_permissions.yaml`). After changing those, regenerate it with

```shell
python dev/convert_role_permissions.py
```

## Deployment

The `Dockerfile` builds the portal and serves it with nginx. The server address is baked in at build time:

```shell
docker build --build-arg VITE_QCFRACTAL_URI=https://qcfractal.example.com -t qcwebportal .
```
