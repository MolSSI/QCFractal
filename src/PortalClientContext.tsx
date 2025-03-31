import {createContext, useContext, useEffect, useState, ReactNode, useCallback} from "react";
import { PortalClient } from "./services/client.ts";
import { ServerInfo, ConnectionState } from "./services/types.ts";

const client = new PortalClient("https://prodtest.qcarchive.molssi.org");

type ClientContextType = {
    connectionState: ConnectionState;
    serverInfo: ServerInfo;

    login: (username: string, password: string) => Promise<void>;
    logout: () => Promise<void>;

    client: PortalClient;
};

// Create context with default `undefined` to force usage inside a Provider
const PortalClientContext = createContext<ClientContextType | undefined>(undefined);

export function PortalClientProvider({ children }: { children: ReactNode }) {
    const [connectionState, setConnectionState] = useState<ConnectionState>({
        connected: false,
        userInfo: undefined,
    });

    const [serverInfo, setServerInfo] = useState<ServerInfo>({
        name: "(unknown)",
        version: "(unknown)",
    });

    useEffect(() => { setConnectionState(client.connectionState) }, []);

    async function login(username: string, password: string) {
        try {
            await client.connect(username, password);
        } finally {
            setConnectionState( {...client.connectionState} );
            setServerInfo( {...client.serverInfo} );
        }
    }

    async function logout() {
        try {
            await client.disconnect(true);
        } finally {
            setConnectionState( {...client.connectionState} );
            setServerInfo( {...client.serverInfo} );
        }
    }

    return (
        <PortalClientContext.Provider value={{ connectionState, serverInfo, login, logout, client }}>
            {children}
        </PortalClientContext.Provider>
    );
}

// Hook for authentication-related functionality
export function usePortalClientAuth() {
    const context = useContext(PortalClientContext);
    if (!context) {
        throw new Error("usePortalClientAuth must be used within a PortalClientProvider");
    }
    const { connectionState, serverInfo, login, logout } = context;
    return { connectionState, serverInfo, login, logout };
}

// Hook for accessing the client instance
export function usePortalClient() {
    const context = useContext(PortalClientContext);
    if (!context) {
        throw new Error("useClient must be used within a PortalClientProvider");
    }
    const { connectionState, serverInfo, client } = context;
    return { connectionState, serverInfo, client };
}

export function usePortalClientFetch<T>(
    fetchFunction: () => Promise<T>,
    connectionState: ConnectionState,
    dependencies: unknown[] = [] // Allows automatic re-fetching when dependencies change
) {
    const [data, setData] = useState<T | null>(null);
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState<string | null>(null);


    const memoizedFunction = useCallback(fetchFunction, dependencies);
    const fetchData = useCallback(async () => {
        setLoading(true);
        setError(null);
        setData(null);

        if (!connectionState.connected) {
            setError("Client is not connected");
            setLoading(false);
        }
        else {
            try {
                const result = await memoizedFunction();
                setData(result);
            } catch (err) {
                setError(`Failed to fetch data: ${err instanceof Error ? err.message : "Unknown error"}`);
                throw err;
            } finally {
                setLoading(false);
            }
        }
    }, [connectionState, memoizedFunction]);

    // Re-fetch data when dependencies change (but not infinitely)
    useEffect(() => {
        fetchData();
    }, [fetchData, connectionState, ...dependencies]); // Dependencies trigger updates safely

    return { data, loading, error, refresh: fetchData };
}
