# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

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

There are no test scripts in this project.

## Environment

The app requires a `VITE_QCFRACTAL_URI` environment variable pointing to the QCFractal server. Set this in a `.env.local` file for local development:

```
VITE_QCFRACTAL_URI=http://localhost:7777
```

## Backend API Reference

`@dev/qcfractal_openapi_spec.json` contains the full OpenAPI spec for the QCFractal backend. Use this to look up available endpoints, request/response schemas, and query parameters. This file is gitignored and local-only.

## Architecture

This is a React 19 + TypeScript + Vite SPA for the QCFractal quantum chemistry compute platform, deployed to Azure Static Web Apps. All routes are lazy-loaded via React Suspense.

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

`global_role_permissions.json` defines what actions each role can perform. `useAuth().has_permission(action)` checks against the logged-in user's role.

### Deployment

CI/CD deploys to Azure Static Web Apps on push to `main`. `staticwebapp.config.json` rewrites all non-asset routes to `/` for SPA support. `VITE_QCFRACTAL_URI` is injected at build time via GitHub Actions secrets.
