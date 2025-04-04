import {Navigate, Outlet} from "react-router-dom";
import {usePortalClientAuth} from "./usePortalClient"

function ProtectedRoute() {
    const { connectionState } = usePortalClientAuth(); // Get authentication status

    const loading = (!connectionState)
    const not_connected = (connectionState && !connectionState!.connected)
    const not_authorized = (connectionState && connectionState!.connected && !connectionState!.authorized)

    return  (
        <>
            { loading ? (<p>Loading...</p>) :
                not_connected ? (<p>Not connected...</p>) :
                    not_authorized ? (<Navigate to={`/login?redirect=${encodeURIComponent(location.pathname + location.search)}`} />) :
                        <Outlet />
            }
        </>
    )

}

export default ProtectedRoute;
