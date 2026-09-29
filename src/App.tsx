import { lazy, Suspense } from "react";
import { BrowserRouter as Router, Route, Routes } from "react-router-dom";
import { PortalClientProvider } from "./PortalClient.tsx";
import { AuthProvider } from "./Auth.tsx";
const SandboxPage = lazy(() => import("./pages/Sandbox.tsx"));
const HomePage = lazy(() => import("./pages/Home.tsx"));
const LoginPage = lazy(() => import("./pages/Login"));
const MainLayout = lazy(() => import("./layouts/MainLayout"));
const ProtectedRoute = lazy(() => import("./ProtectedRoute"));
const Project = lazy(() => import("./pages/Project.tsx"));
const ProjectList = lazy(() => import("./pages/ProjectList.tsx"));
const Record = lazy(() => import("./pages/Record.tsx"));
const Manager = lazy(() => import("./pages/Manager.tsx"));
const ManagerList = lazy(() => import("./pages/ManagerList.tsx"));
const InternalJobList = lazy(() => import("./pages/InternalJobList.tsx"));
const ServerErrorList = lazy(() => import("./pages/ServerErrorList.tsx"));
const ServerStats = lazy(() => import("./pages/ServerStats.tsx"));
const Dataset = lazy(() => import("./pages/Dataset.tsx"));
const DatasetList = lazy(() => import("./pages/DatasetList.tsx"));
const ApiInfo = lazy(() => import("./pages/./APIInfo"));
const ApiKeys = lazy(() => import("./pages/ApiKeys.tsx"));
const UserProfile = lazy(() =>
  import("./pages/UserProfile.tsx").then((m) => ({ default: m.UserProfile })),
);
const UserList = lazy(() =>
  import("./pages/UserList.tsx").then((m) => ({ default: m.UserList })),
);
const AddProjectRecord = lazy(
  () => import("./components/AddProjectRecord.tsx"),
);
const ThemePlaygroundPage = lazy(() => import("./pages/ThemePlayground.tsx"));
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { PreferencesProvider } from "./PreferencesProvider.tsx";
import { CssBaseline } from "@mui/material";
import AppTheme from "./shared-theme/AppTheme";

import {
  chartsCustomizations,
  dataGridCustomizations,
  treeViewCustomizations,
} from "./theme/customizations";

const xThemeComponents = {
  ...chartsCustomizations,
  ...dataGridCustomizations,
  ...treeViewCustomizations,
};

const queryClient = new QueryClient();

function App() {
  return (
    <AppTheme themeComponents={xThemeComponents}>
      <CssBaseline enableColorScheme />
      <AuthProvider>
        <PortalClientProvider>
          <QueryClientProvider client={queryClient}>
            <PreferencesProvider>
              <Router>
                <Suspense fallback={<div>Loading...</div>}>
                  <Routes>
                    <Route path="/login" element={<LoginPage />} />
                    <Route element={<ProtectedRoute />}>
                      <Route element={<MainLayout />}>
                        <Route path="/" element={<HomePage />} />
                        <Route path="/sandbox" element={<SandboxPage />} />
                        <Route
                          path="/themeplayground"
                          element={<ThemePlaygroundPage />}
                        />
                        <Route path="/me" element={<UserProfile />} />
                        <Route path="/me/api_keys" element={<ApiKeys />} />
                        <Route path="/users" element={<UserList />} />
                        <Route
                          path="/users/:userName"
                          element={<UserProfile />}
                        />
                        <Route path="/projects" element={<ProjectList />} />
                        <Route
                          path="/projects/:projectId"
                          element={<Project />}
                        />
                        <Route
                          path="/projects/:projectId/records/:recordId"
                          element={<Record />}
                        />
                        <Route path="/records/:recordId" element={<Record />} />
                        <Route
                          path="/projects/:projectId/addRecord"
                          element={<AddProjectRecord />}
                        />
                        <Route path="/managers" element={<ManagerList />} />
                        <Route
                          path="/internal_jobs"
                          element={<InternalJobList />}
                        />
                        <Route
                          path="/server_errors"
                          element={<ServerErrorList />}
                        />
                        <Route path="/server_stats" element={<ServerStats />} />
                        <Route
                          path="/managers/:managerName"
                          element={<Manager />}
                        />
                        <Route path="/datasets" element={<DatasetList />} />
                        <Route
                          path="/datasets/:datasetId"
                          element={<Dataset />}
                        />
                        <Route path="/api_access" element={<ApiInfo />} />
                      </Route>
                    </Route>
                  </Routes>
                </Suspense>
              </Router>
            </PreferencesProvider>
          </QueryClientProvider>
        </PortalClientProvider>
      </AuthProvider>
    </AppTheme>
  );
}

export default App;
