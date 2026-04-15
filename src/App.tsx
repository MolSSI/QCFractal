import { lazy, Suspense } from "react";
import { BrowserRouter as Router, Route, Routes } from "react-router-dom";
import { PortalClientProvider } from "./PortalClient.tsx";
import { AuthProvider } from "./Auth.tsx";
const SandboxPage  = lazy(() => import("./pages/Sandbox.tsx"));
const HomePage = lazy(() => import("./pages/Home.tsx"));
const LoginPage = lazy(() => import("./pages/Login"));
const MainLayout = lazy(() => import("./layouts/MainLayout"));
const ProtectedRoute = lazy(() => import("./ProtectedRoute"));
const Project = lazy(() => import("./pages/Project.tsx"));
const ProjectList = lazy(() => import("./pages/ProjectList.tsx"));
const Record = lazy(() => import("./pages/Record.tsx"));
const Manager = lazy(() => import("./pages/Manager.tsx"));
const ManagerList = lazy(() => import("./pages/ManagerList.tsx"));
const Dataset = lazy(() => import("./pages/Dataset.tsx"));
const DatasetList = lazy(() => import("./pages/DatasetList.tsx"));
const UserInfo = lazy(() => import("./pages/UserInfo.tsx").then(m => ({ default: m.UserInfo })));
const AddProjectRecord = lazy(() => import("./components/AddProjectRecord.tsx"));
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
                        <Route path="/themeplayground" element={<ThemePlaygroundPage />} />
                        <Route path="/me" element={<UserInfo />} />
                        <Route path="/users/:userName" element={<UserInfo />} />
                        <Route path="/projects" element={<ProjectList />} />
                        <Route path="/projects/:projectId" element={<Project />} />
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
                          path="/managers/:managerName"
                          element={<Manager />}
                        />
                        <Route path="/datasets" element={<DatasetList />} />
                        <Route path="/datasets/:datasetId" element={<Dataset />} />
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
