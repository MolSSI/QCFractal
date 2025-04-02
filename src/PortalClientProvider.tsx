import {ReactNode, useCallback, useState} from "react";
import * as qcpTypes from "./PortalTypes";
import * as requestHelpers from "./RequestHelpers";
import * as authHelpers from "./AuthHelpers";
import {PortalClientContext, RequestReturnType} from "./PortalClientContext";

export function PortalClientProvider({children}: { children: ReactNode }) {
    const [connectionState, setConnectionState] = useState<qcpTypes.ConnectionState>({
        connected: false,
        userInfo: undefined,
    });

    const [serverInfo, setServerInfo] = useState<qcpTypes.ServerInfo>({
        name: "(unknown)",
        version: "(unknown)",
    });


    const logout = useCallback(async function (force: boolean): Promise<void> {
            if (force || connectionState.connected) {
                await authHelpers.logout()
                setConnectionState({connected: false, userInfo: undefined});
                setServerInfo({name: "(unknown)", version: "(unknown)"});
            }
        }, [connectionState.connected]
    );

    const login = useCallback(async function (username?: string, password?: string): Promise<void> {
            if (connectionState.connected) {
                await logout(false);
            }

            const {serverInfo, userInfo} = await authHelpers.login(username, password);
            setConnectionState({connected: true, userInfo: userInfo});
            setServerInfo({...serverInfo});
        }, [logout, connectionState.connected]
    );

    const wrappedMakeRequest = useCallback(
        async function <T>(method: string, endpoint: string, body?: object, url_params?: Record<string, string>): Promise<RequestReturnType<T>> {
            try {
                const r = await requestHelpers.rawMakeRequest<T>(method, endpoint, body, url_params);
                return {data: r, error: undefined};
            } catch (err) {
                const errmsg = `Failed to request data: ${err instanceof Error ? err.message : String(err)}`
                return {data: undefined, error: errmsg} as RequestReturnType<T>
            }
        },
        []
    );

    return (
        <PortalClientContext.Provider
            value={{connectionState, serverInfo, login, logout, makeRequest: wrappedMakeRequest}}>
            {children}
        </PortalClientContext.Provider>
    );
}

