# AGENTS.md

This file provides guidance to AI agents when working with the QCFractal web portal. The web portal lives in the
`qcwebportal/` directory of the QCFractal repository; the QCFractal server it talks to is in `../qcfractal`.
All paths and commands below are relative to `qcwebportal/`.

## Commands

```bash
npm run dev          # Start dev server with HMR
npm run build        # Type-check (tsc -b) then build with Vite
npm run devbuild     # Build without type-checking (faster iteration)
npm run typecheck    # Type-check only, no emit
npm run lint         # Run ESLint
npm run format       # Format with Prettier
npm run preview      # Preview production build
```

There are no test scripts in this project. CI (`.github/workflows/webportal_build.yml` at the repository root) runs
`npm run build` and checks that `src/global_role_permissions.json` matches the server.

## Environment

The app requires a `VITE_QCFRACTAL_URI` environment variable pointing to the QCFractal server. Set this in a `.env.local` file for local development:

```
VITE_QCFRACTAL_URI=http://localhost:7777
```

## Backend API Reference

`dev/qcfractal_openapi_spec.json` contains the full OpenAPI spec for the QCFractal backend (a snapshot of the
server's `/api/v1/openapi` endpoint). Use this to look up available endpoints, request/response schemas, and query
parameters. Keep it updated when backend endpoints change. The server source is also available in this repository
(routes are in `../qcfractal/qcfractal/components/*/routes.py`).

`dev/endpoint_permission_map.json` maps endpoints and HTTP methods to the resource and action required to access them.

## Architecture

This is a React 19 + TypeScript + Vite SPA for the QCFractal quantum chemistry compute platform. All routes are lazy-loaded via React Suspense.

### Source Layout

```
src/
├── App.tsx                    # Router + provider stack
├── Auth.tsx                   # Auth context
├── PortalClient.tsx           # API client context
├── PreferencesProvider.tsx    # User preferences context
├── ProtectedRoute.tsx         # Redirects to /login if not authorized
├── PortalTypes.ts             # Re-exports all API types
├── portal_types/              # Type definitions split by domain
│   ├── common.ts              # Shared types (User, Manager, Project, Dataset, etc.)
│   ├── record_types.ts        # RecordType enum + RecordData union
│   ├── singlepoint.ts
│   ├── optimization.ts
│   ├── torsiondrive.ts
│   ├── gridoptimization.ts
│   ├── reaction.ts
│   ├── manybody.ts
│   └── neb.ts
├── pages/                     # Route-level page components (14 pages)
├── components/                # Reusable UI components
│   ├── dataset_components/    # Dataset-specific components
│   ├── project_components/    # Project-specific components
│   └── record_components/     # Per-record-type renderers
├── layouts/                   # MainLayout (sidebar + header + outlet)
├── RequestHelpers.ts          # Low-level HTTP helpers
├── request_config.ts          # server_address + default headers
├── Exceptions.ts              # AuthenticationError, AuthorizationError
├── Utils.ts                   # Shared utility functions
├── MoleculeUtils.ts           # Molecule SDF conversion
├── global_role_permissions.json  # Permission matrix by role
└── shared-theme/ + theme/     # MUI theme config and customizations
```

### Context Provider Hierarchy

`App.tsx` wraps the app in nested providers (order matters):

1. **`AppTheme`** — MUI theme with customizations from `src/theme/customizations/`
2. **`AuthProvider`** (`Auth.tsx`) — Session auth state, login/logout, server connectivity. On load, calls `/api/v1/ping` to detect login status. Exposes `useAuth()`.
3. **`PortalClientProvider`** (`PortalClient.tsx`) — Intercepts 401s and retriggers `ping()`. Exposes `usePortalClient()` which returns `makeRequest<T>(method, endpoint, body?, url_params?)`.
4. **`QueryClientProvider`** — TanStack React Query for data fetching/caching.
5. **`PreferencesProvider`** (`PreferencesProvider.tsx`) — User preferences stored server-side at `/api/v1/me/preferences`. Full prefs object is fetched/replaced on every update (no partial update endpoint). Exposes `usePreferences()`.

### API Communication

All components should use `makeRequest` from `usePortalClient()` rather than calling request helpers directly:

```typescript
const { makeRequest } = usePortalClient();
const data = await makeRequest<ResponseType>("GET", "api/v1/endpoint", undefined, { param: value });
```

For file uploads, pass `FormData` as the body (do not set `Content-Type` manually).

### Routing

All authenticated routes are nested under `<ProtectedRoute>` → `<MainLayout>`. Key routes:

| Path | Page |
|------|------|
| `/` | `HomePage` — dashboard with favorited projects/datasets/records |
| `/projects` | `ProjectList` |
| `/projects/:projectId` | `Project` (tabs: Datasets, Records) |
| `/projects/:projectId/records/:recordId` | `Record` |
| `/projects/:projectId/addRecord` | `AddProjectRecord` |
| `/records/:recordId` | `Record` (direct link) |
| `/datasets` | `DatasetList` |
| `/datasets/:datasetId` | `Dataset` (tabs: Status, Specs, Entries, Records, Attachments) |
| `/managers` | `ManagerList` |
| `/managers/:managerName` | `Manager` |
| `/internal_jobs` | `InternalJobList` |
| `/server_errors` | `ServerErrorList` |
| `/me`, `/users/:userName` | `UserInfo` |

### Data Fetching Pattern

React Query is used throughout. Standard pattern:

```typescript
const { data, isLoading, error } = useQuery({
  queryKey: ["entityType", id, filter],
  queryFn: () => makeRequest<T>("GET", "api/v1/endpoint", undefined, { id }),
  enabled: !!id,
});
```

Mutations follow the pattern of fetching current state, modifying, then PUTting the full object (no PATCH endpoints). Cache invalidation is done via `queryClient.invalidateQueries()`.

### Record Types

Seven computation record types, each with a dedicated renderer under `src/components/record_components/`:
`singlepoint`, `optimization`, `torsiondrive`, `gridoptimization`, `reaction`, `manybody`, `neb`

`src/Utils.ts:getRecordReprMolecule()` maps each type to its representative molecule field.

### Molecule Visualization

`src/components/Molecule.tsx` uses the NGL library. Components must be wrapped in `<MoleculeStageProvider width height>` before using `<MoleculeViewer moleculeData={...}>`.

### Permissions

`src/global_role_permissions.json` defines what actions each role can perform on each resource.
`useAuth().has_permission(resource, action)` checks against the logged-in user's role.

The json file is generated from the server's `../qcfractal/qcfractal/components/auth/global_role_permissions.yaml`
by `dev/convert_role_permissions.py`. Do not edit it by hand; change the server's yaml and rerun the script.

### Changelog

The home page shows a "What's New / Changelog" section, rendered by `src/components/Changelog.tsx` from the `CHANGELOG` data array. Keep it up to date: when you add or change a user-facing feature, add a corresponding entry.

- Add an item to the matching `date` entry, or add a new dated entry at the top of `CHANGELOG` (entries are newest first, `date` formatted `YYYY-MM-DD`). Each item is `{ kind: "Added" | "Improved" | "Fixed", text }`.
- The section is collapsed by default and its header shows the newest entry's date as "Last updated" — no markup changes are needed when adding an entry.
- Only include user-facing changes. Skip purely internal refactors, and if you are unsure whether a change warrants an entry, ask.

### Deployment

The portal is deployed as a Docker image (`Dockerfile`), served by nginx (`docker/nginx.conf`), which falls back to
`index.html` for all non-file routes for SPA support. `VITE_QCFRACTAL_URI` and `VITE_FEEDBACK_URL` are baked in at
build time, either as Docker build args or from a `.env.production`/`.env.local` file in the build context.
