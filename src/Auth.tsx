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
import global_role_permissions from "./global_role_permissions.json";

const all_role_permissions = global_role_permissions as Record<string, any>;

type ServerStatus = "loading" | "disconnected" | "connected";

function evaluate_global_permissions(
  role: string,
  resource: string,
  action: string,
): boolean {
  if (!(role in global_role_permissions)) {
    return false;
  }

  const role_permissions = all_role_permissions[role];

  // Check if there is an entry for this resource or if there is a wildcard entry for all resources.
  // If not, deny by default.
  let resource_permissions;
  if (resource in role_permissions) {
    resource_permissions = role_permissions[resource];
  } else if ("*" in role_permissions) {
    resource_permissions = role_permissions["*"];
  } else {
    return false;
  }

  // Now check the action (or wildcard) and return whatever is there
  let permission;
  if (action in resource_permissions) {
    permission = resource_permissions[action];
  } else if ("*" in resource_permissions) {
    permission = resource_permissions["*"];
  } else {
    return false;
  }

  return permission === "Allow";
}

type AuthContextType = {
  serverStatus: ServerStatus;
  serverInfo: qcpTypes.ServerInfo;

  authorized: boolean;
  userInfo?: qcpTypes.UserInfo;

  ping: () => Promise<qcpTypes.PingResults | undefined>;
  fetchServerInfo: () => Promise<qcpTypes.ServerInfo | undefined>;
  login: (username?: string, password?: string) => Promise<void>;
  logout: () => Promise<void>;
  loggedIn: boolean;

  has_permission: (resource: string, action: string) => boolean;
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

  const ping = useCallback(async (): Promise<
    qcpTypes.PingResults | undefined
  > => {
    try {
      const r = await rawMakeRequest<qcpTypes.PingResults>(
        "get",
        `api/v1/ping`,
      );
      setServerStatus("connected");
      setAuthorized(r.authorized);
      setUserInfo(r.user_info);
      return r;
    } catch {
      setServerStatus("disconnected");
      setAuthorized(false);
      setUserInfo(undefined);
      return undefined;
    }
  }, []);

  const fetchServerInfo: () => Promise<qcpTypes.ServerInfo | undefined> =
    useCallback(async () => {
      try {
        const sInfo = await rawMakeRequest<qcpTypes.ServerInfo>(
          "get",
          `api/v1/information`,
        );
        setServerInfo(sInfo);
        return sInfo;
      } catch (e) {
        setServerInfo({
          name: "(unknown)",
          version: "(unknown)",
        } as qcpTypes.ServerInfo);
        console.warn("Failed to fetch server info:", e);
        return undefined;
      }
    }, []);

  const logout = useCallback(async () => {
    const logout_url: string = `${server_address}/auth/v1/session_logout`;

    const req_options: RequestInit = {
      method: "POST",
      headers: server_headers,
      credentials: "include",
    };

    try {
      if (userInfo) {
        await rawRequest<void>(logout_url, req_options);
      }
    } catch (err) {
      console.warn("Logout failed:", err);
    } finally {
      await ping(); // sets user info
    }
  }, [userInfo, ping]);

  const login = useCallback(
    async (username?: string, password?: string) => {
      setUserInfo(undefined);
      setAuthorized(false);

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

      const pingResults = await ping();
      if (!pingResults?.authorized) {
        throw new Error("Login failed or not authorized");
      }

      await fetchServerInfo();
    },
    [fetchServerInfo, ping],
  );

  const loggedIn = !!userInfo;

  const has_permission = (resource: string, action: string): boolean => {
    return evaluate_global_permissions(
      userInfo?.role || "anonymous",
      resource,
      action,
    );
  };

  // Detect login status & server info on initial page load
  useEffect(() => {
    const f = async () => {
      const r = await ping();

      if (r?.authorized) {
        await fetchServerInfo();
      }
    };
    f().catch(console.error);
  }, [ping, fetchServerInfo]);

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
        loggedIn,
        has_permission,
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
    loggedIn,
    has_permission,
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
    loggedIn,
    has_permission,
  };
}