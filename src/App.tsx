import { PortalClientProvider} from "./PortalClientContext";
import SandboxPage  from "./pages/Sandbox";
import LoginPage from "./pages/Login";
import './App.css'

function App() {
  return (
    <PortalClientProvider>
        <LoginPage />
    </PortalClientProvider>
  )
}

export default App
