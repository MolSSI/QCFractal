import {PortalClientProvider} from "./PortalClientProvider";
import MainPage from "./pages/Main"
import LoginPage from "./pages/Login";
import MainLayout from "./layouts/MainLayout";
import {BrowserRouter as Router, Navigate, Route, Routes} from "react-router-dom";
import './App.css'
import SandboxPage from "./components/Sandbox.tsx";
import Project from "./components/Project.tsx";
import React from "react";

function App() {
    return (
        <PortalClientProvider>
            <Router>
                <Routes>
                    <Route path="/login" element={<LoginPage />} />

                    <Route element={<MainLayout />}>
                        <Route path="/" element={<MainPage />} />
                        <Route path="/sandbox" element={<SandboxPage />} />
                        <Route path="/projects/:projectId" element={<Project />}  />
                        <Route path="*" element={<Navigate to="/login" />} />
                    </Route>

                    <Route path="*" element={<Navigate to="/login" />} />
                </Routes>
            </Router>
        </PortalClientProvider>
    );
}

export default App
