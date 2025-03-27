import { useClient } from '../ClientHook.tsx'

export default function AuthStatus() {
    const { clientStatus, login, logout } = useClient();

    return (
        <div>
            {clientStatus.connected ? (
                <>
                    <p>{clientStatus.serverInfo?.name}</p>
                    <p>v{clientStatus.serverInfo?.version}</p>
                    <p>User: {clientStatus.username ? clientStatus.username : 'anonymous'}</p>
                    <button onClick={logout}>Logout</button>
                </>
            ) : (
                <>
                    <p>Not logged in</p>
                    <button onClick={() => login("ben", "ben1234")}>Login</button>
                </>
            )}
        </div>
    );
}