import { usePortalClientAuth } from "../PortalClientContext";

export default function AuthStatus() {
    const { connectionState, login, logout } = usePortalClientAuth();

    return (
        <div>
            {connectionState.connected ? (
                <>
                    <p>User: {connectionState.userInfo?.username ? connectionState.userInfo?.username : 'anonymous'}</p>
                    <button onClick={logout}>Logout</button>
                </>
            ) : (
                <>
                    <p>Not logged in</p>
                    <button onClick={() => login("guitest", "xxxxxxx")}>Login</button>
                </>
            )}
        </div>
    );
}

//<p>{clientStatus.serverInfo?.name}</p>
//<p>v{clientStatus.serverInfo?.version}</p>
