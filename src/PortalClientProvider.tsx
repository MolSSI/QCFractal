import { ReactNode, useCallback, useEffect, useState } from "react";
import * as qcpTypes from "./PortalTypes";
import * as requestHelpers from "./RequestHelpers";
import * as authHelpers from "./AuthHelpers";
import { PortalClientContext, RequestReturnType } from "./PortalClientContext";
import { AuthenticationError } from "./Exceptions";

export function PortalClientProvider({ children }: { children: ReactNode }) {
  const [connectionState, setConnectionState] = useState<
    qcpTypes.ConnectionState | undefined
  >(undefined);

  const [serverInfo, setServerInfo] = useState<qcpTypes.ServerInfo>({
    name: "(unknown)",
    version: "(unknown)",
  });

  const logout = useCallback(
    async function (force: boolean): Promise<void> {
      if (force || (connectionState && connectionState.connected)) {
        await authHelpers.logout();
        setConnectionState({
          connected: true,
          authorized: false,
          userInfo: undefined,
        });
        setServerInfo({ name: "(unknown)", version: "(unknown)" });
      }
    },
    [connectionState],
  );

  const login = useCallback(
    async function (username?: string, password?: string): Promise<void> {
      if (
        connectionState &&
        connectionState.connected &&
        connectionState.userInfo
      ) {
        await logout(false);
      }

      const { serverInfo, userInfo } = await authHelpers.login(
        username,
        password,
      );
      setConnectionState({
        connected: true,
        authorized: true,
        userInfo: userInfo,
      });
      setServerInfo({ ...serverInfo });
    },
    [logout, connectionState],
  );

  const wrappedMakeRequest = useCallback(
    async function <T>(
      method: string,
      endpoint: string,
      body?: object,
      url_params?: Record<string, string>,
    ): Promise<RequestReturnType<T>> {
      try {
        const r = await requestHelpers.rawMakeRequest<T>(
          method,
          endpoint,
          body,
          url_params,
        );
        return { data: r, error: undefined };
      } catch (err) {
        if (err instanceof AuthenticationError) {
          if (connectionState && connectionState.connected) {
            setConnectionState((p) => ({ ...p!, authorized: false }));
          }
        }
        const errmsg = `Failed to request data: ${err instanceof Error ? err.message : String(err)}`;
        return { data: undefined, error: errmsg } as RequestReturnType<T>;
      }
    },
    [connectionState],
  );

  // Detect login status & server info on initial load
  useEffect(() => {
    const f = async () => {
      const { connected, authorized, userInfo } =
        await requestHelpers.checkLoginState();
      setConnectionState({ connected, authorized, userInfo });

      if (connected && authorized) {
        const sinfo = await requestHelpers.serverInfo();
        if (sinfo) {
          setServerInfo(sinfo);
        }
      }
    };
    f().catch(console.error);
  }, []);

  return (
    <PortalClientContext.Provider
      value={{
        connectionState,
        serverInfo,
        login,
        logout,
        makeRequest: wrappedMakeRequest,
      }}
    >
      {children}
    </PortalClientContext.Provider>
  );
}
