import { BrowserRouter as Router, Route, Routes } from "react-router-dom";
import { PortalClientProvider } from "./PortalClient.tsx";
import { AuthProvider } from "./Auth.tsx";
import HomePage from "./pages/Home.tsx";
import LoginPage from "./pages/Login";
import MainLayout from "./layouts/MainLayout";
import ProtectedRoute from "./ProtectedRoute";
import Project from "./components/Project.tsx";
import ProjectList from "./components/ProjectList.tsx";
import Record from "./components/Record.tsx";
import Manager from "./components/Manager.tsx";
import ManagerList from "./components/ManagerList.tsx";
import { UserInfo } from "./components/UserInfo.tsx";
import AddProjectRecord from "./components/AddProjectRecord.tsx";

function App() {
  return (
    <AuthProvider>
      <PortalClientProvider>
        <Router>
          <Routes>
            <Route path="/login" element={<LoginPage />} />
            <Route element={<ProtectedRoute />}>
              <Route element={<MainLayout />}>
                <Route path="/" element={<HomePage />} />
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
                <Route path="/managers/:managerName" element={<Manager />} />
              </Route>
            </Route>
          </Routes>
        </Router>
      </PortalClientProvider>
    </AuthProvider>
  );
}

export default App;
