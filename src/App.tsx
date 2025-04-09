import { BrowserRouter as Router, Route, Routes } from "react-router-dom";
import { PortalClientProvider } from "./PortalClientProvider";
import HomePage from "./pages/Home.tsx";
import LoginPage from "./pages/Login";
import MainLayout from "./layouts/MainLayout";
import ProtectedRoute from "./ProtectedRoute";
import Project from "./components/Project.tsx";
import CalculationRecord from "./components/CalculationRecord";
import ProjectList from "./components/ProjectList.tsx";

function App() {
  return (
    <PortalClientProvider>
      <Router>
        <Routes>
          <Route path="/login" element={<LoginPage />} />
          <Route element={<ProtectedRoute />}>
            <Route element={<MainLayout />}>
              <Route path="/" element={<HomePage />} />
              <Route path="/projects" element={<ProjectList />} />
              <Route path="/projects/:projectId" element={<Project />} />
              <Route
                path="/projects/:projectId/records/:recordId"
                element={<CalculationRecord />}
              />
            </Route>
          </Route>
        </Routes>
      </Router>
    </PortalClientProvider>
  );
}

export default App;
