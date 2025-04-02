import { PortalClientProvider} from "./PortalClientContext";
import {BrowserRouter as Router, Routes,Route,Navigate} from "react-router-dom";
import SandboxPage  from "./pages/Sandbox";
import LoginPage from "./pages/Login";
import './App.css'

function App() {
    return (
        <PortalClientProvider>
            <Router>
                <Routes>
                    <Route path="/login" element={<LoginPage />} />
                    <Route path="/sandbox" element={<SandboxPage />} />
                    <Route path="*" element={<Navigate to="/login" />} />
                </Routes>
            </Router>
        </PortalClientProvider>
    );
}

export default App
