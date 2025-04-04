import {BrowserRouter as Router, Route, Routes} from "react-router-dom";
import {PortalClientProvider} from "./PortalClientProvider";
import MainPage from "./pages/Main"
import LoginPage from "./pages/Login";
import MainLayout from "./layouts/MainLayout";
import ProtectedRoute from "./ProtectedRoute";
import SandboxPage from "./components/Sandbox.tsx";
import Project from "./components/Project.tsx";
import CalculationRecord from "./components/CalculationRecord";
import './App.css'

function App() {
    return (
        <PortalClientProvider>
            <Router>
                <Routes>
                    <Route path="/login" element={<LoginPage />} />
                    <Route element={<ProtectedRoute />}>
                        <Route element={<MainLayout />}>
                            <Route path="/" element={<MainPage />} />
                            <Route path="/sandbox" element={<SandboxPage />} />
                            <Route path="/projects/:projectId" element={<Project />}  />
                            <Route path="/projects/:projectId/records/:recordId" element={<CalculationRecord />}  />
                        </Route>
                    </Route>
                </Routes>
            </Router>
        </PortalClientProvider>
    );
}

export default App
