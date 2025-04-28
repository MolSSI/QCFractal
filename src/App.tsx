import { BrowserRouter as Router, Route, Routes } from "react-router-dom";
import { PortalClientProvider } from "./PortalClientProvider";
import HomePage from "./pages/Home.tsx";
import LoginPage from "./pages/Login";
import MainLayout from "./layouts/MainLayout";
import ProtectedRoute from "./ProtectedRoute";
import Project from "./components/Project.tsx";
import ProjectList from "./components/ProjectList.tsx";
import Record from "./components/Record.tsx";
import Manager from "./components/Manager.tsx";
import ManagerList from "./components/ManagerList.tsx";
import {MyUserInfo, UserInfo} from "./components/UserInfo.tsx";

function App() {
  return (
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
              <Route path="/projects/:projectId/records/:recordId" element={<Record />} />
              <Route path="/managers" element={<ManagerList />} />
              <Route path="/managers/:managerName" element={<Manager />} />
            </Route>
          </Route>
        </Routes>
      </Router>
    </PortalClientProvider>
  );
}

export default App;
