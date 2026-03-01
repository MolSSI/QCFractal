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

## Architecture

This is a React + TypeScript + Vite single-page application for the QCFractal quantum chemistry compute platform, deployed to Azure Static Web Apps.

### Context Provider Hierarchy

`App.tsx` wraps the app in nested providers (order matters):

1. **`AppTheme`** — MUI theme with customizations from `src/theme/customizations/`
2. **`AuthProvider`** (`src/Auth.tsx`) — Session auth state, login/logout, server connectivity. On load, calls `/api/v1/ping` to detect login status.
3. **`PortalClientProvider`** (`src/PortalClient.tsx`) — Wraps `makeRequest()` to intercept 401s and trigger auth re-check via `ping()`.
4. **`QueryClientProvider`** — TanStack React Query for data fetching/caching.
5. **`PreferencesProvider`** (`src/PreferencesProvider.tsx`) — User preferences stored server-side at `/api/v1/me/preferences`. Full prefs object is fetched/replaced on every update (no partial update endpoint).

### API Communication

- `src/request_config.ts` — Exports `server_address` (`VITE_QCFRACTAL_URI`) and default headers.
- `src/RequestHelpers.ts` — `rawRequest()` and `rawMakeRequest()` handle HTTP and error mapping. 401 → `AuthenticationError`, 403 → `AuthorizationError` (both from `src/Exceptions.ts`).
- `usePortalClient()` hook from `src/PortalClient.tsx` exposes `makeRequest()` for use in components — all components should use this rather than calling request helpers directly.

### Routing

All authenticated routes are nested under `<ProtectedRoute>` → `<MainLayout>`. `MainLayout` renders a persistent `<SideMenu>` on the left and `<Header>` at the top, with the page content via `<Outlet>`.

Key routes:
- `/` — Home (sandbox/dev page)
- `/projects` — `ProjectList`
- `/projects/:projectId` — `Project` (tabs: Datasets, Records)
- `/projects/:projectId/records/:recordId` — `Record`
- `/records/:recordId` — `Record` (direct link)
- `/managers` / `/managers/:managerName` — `Manager` / `ManagerList`
- `/me`, `/users/:userName` — `UserInfo`

### Data Types

All QCFractal API types are in `src/PortalTypes.ts`. Key types: `RecordData`, `Project`, `ProjectListEntry`, `Manager`, `Molecule`, `UserInfo`, `UserPreferences`.

### Record Types

`src/Utils.ts` has `getRecordReprMolecule()` which maps record types to their representative molecule field:
- `singlepoint` → `molecule_id`
- `optimization` → `initial_molecule_id`
- `torsiondrive` → `initial_molecules_id[0]`
- `gridoptimization`, `manybody` → `initial_molecule_id`
- `neb` → `initial_chain[0]`
- `reaction` → `undefined`

### Molecule Visualization

`src/components/Molecule.tsx` uses the NGL library. Components must be wrapped in `<MoleculeStageProvider width height>` before using `<MoleculeViewer moleculeData={...}>`.

### Deployment

CI/CD deploys to Azure Static Web Apps on push to `main`. The `VITE_QCFRACTAL_URI` is injected at build time via GitHub Actions secrets. `staticwebapp.config.json` rewrites all non-asset routes to `/` for SPA support.
