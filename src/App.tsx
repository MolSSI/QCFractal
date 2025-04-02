import { PortalClientProvider} from "./PortalClientProvider";
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
