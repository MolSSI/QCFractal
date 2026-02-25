import { Navigate, Outlet } from "react-router-dom";
import { useAuth } from "./Auth.tsx";

function ProtectedRoute() {
  const { authorized, serverStatus } = useAuth(); // Get authentication status

  return (
    <>
      {serverStatus == "loading" ? (
        <p>Loading...</p>
      ) : serverStatus == "disconnected" ? (
        <p>Not connected...</p>
      ) : !authorized ? (
        <Navigate
          to={`/login?redirect=${encodeURIComponent(location.pathname + location.search)}`}
        />
      ) : (
        <Outlet />
      )}
    </>
  );
}

export default ProtectedRoute;
