import {
  createContext,
  ReactNode,
  useCallback,
  useContext,
  useEffect,
  useState,
} from "react";
import * as qcpTypes from "./PortalTypes.ts";
import { rawMakeRequest, rawRequest } from "./RequestHelpers.ts";
import { server_address, server_headers } from "./request_config.ts";

type ServerStatus = "loading" | "disconnected" | "connected";

type AuthContextType = {
  serverStatus: ServerStatus;
  serverInfo: qcpTypes.ServerInfo;

  authorized: boolean;
  userInfo?: qcpTypes.UserInfo;

  ping: () => Promise<void>;
  fetchServerInfo: () => Promise<void>;
  login: (username?: string, password?: string) => Promise<void>;
  logout: () => Promise<void>;
};

export const AuthContext = createContext<AuthContextType | undefined>(
  undefined,
);

export function AuthProvider({ children }: { children: ReactNode }) {
  const [serverStatus, setServerStatus] = useState<ServerStatus>("loading");

  const [serverInfo, setServerInfo] = useState<qcpTypes.ServerInfo>({
    name: "(unknown)",
    version: "(unknown)",
  });

  const [authorized, setAuthorized] = useState<boolean>(false);
  const [userInfo, setUserInfo] = useState<qcpTypes.UserInfo | undefined>(
    undefined,
  );

  const ping = useCallback(async () => {
    try {
      const r = await rawMakeRequest<qcpTypes.PingResults>(
        "get",
        `api/v1/ping`,
      );
      setServerStatus("connected");
      setAuthorized(r.authorized);
      setUserInfo(r.user_info);
    } catch {
      setServerStatus("disconnected");
      setAuthorized(false);
      setUserInfo(undefined);
    }
  }, []);

  const fetchServerInfo = useCallback(async () => {
    try {
      const sInfo = await rawMakeRequest<qcpTypes.ServerInfo>(
        "get",
        `api/v1/information`,
      );
      setServerInfo(sInfo);
    } catch (e) {
      setServerInfo({
        name: "(unknown)",
        version: "(unknown)",
      } as qcpTypes.ServerInfo);
      console.warn("Failed to fetch server info:", e);
    }
  }, []);

  const logout = useCallback(async () => {
    const logout_url: string = `${server_address}/auth/v1/session_logout`;

    if (!userInfo) return;

    const req_options: RequestInit = {
      method: "POST",
      headers: server_headers,
      credentials: "include",
    };

    try {
      // TODO - better error handling
      await rawRequest<void>(logout_url, req_options);
      await ping();
      setUserInfo(undefined);
    } catch (err) {
      console.warn("Logout failed:", err);
    }
  }, [userInfo, ping]);

  const login = useCallback(
    async (username?: string, password?: string) => {
      setUserInfo(undefined);

      if ((username && !password) || (password && !username)) {
        throw new Error(
          "Both username and password must be provided together, or not at all",
        );
      }

      if (username && password) {
        const login_url = `${server_address}/auth/v1/session_login`;

        const body = {
          username: username,
          password: password,
        };

        const req_options: RequestInit = {
          method: "POST",
          headers: server_headers,
          credentials: "include",
          body: JSON.stringify(body),
        };

        // throws an exception on error
        const userInfo = await rawRequest<qcpTypes.UserInfo>(
          login_url,
          req_options,
        );

        setUserInfo(userInfo);
      }

      // else see if we can log in anonymously
      // the below will throw an exception if not logged in and the server requires it
      await fetchServerInfo();
      setAuthorized(true);
    },
    [fetchServerInfo],
  );


  // Detect login status & server info on initial page load
  useEffect(() => {
    const f = async () => {
      await ping();

      if (authorized) {
        await fetchServerInfo();
      }
    };
    f().catch(console.error);
  }, [authorized, ping, fetchServerInfo]);

  return (
    <AuthContext.Provider
      value={{
        ping,
        fetchServerInfo,
        serverStatus,
        serverInfo,
        authorized,
        userInfo,
        login,
        logout,
      }}
    >
      {children}
    </AuthContext.Provider>
  );
}

export function useAuth() {
  const context = useContext(AuthContext);
  if (!context) {
    throw new Error("useAuth must be used within a AuthProvider");
  }

  const {
    ping,
    fetchServerInfo,
    serverStatus,
    serverInfo,
    authorized,
    userInfo,
    login,
    logout,
  } = context;

  return {
    ping,
    fetchServerInfo,
    serverStatus,
    serverInfo,
    authorized,
    userInfo,
    login,
    logout,
  };
}
