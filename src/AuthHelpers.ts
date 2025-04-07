import { server_address, server_headers } from "./request_config";
import * as qcpTypes from "./PortalTypes";
import { rawMakeRequest, rawRequest } from "./RequestHelpers";

export async function logout(): Promise<void> {
  const logout_url: string = `${server_address}/auth/v1/session_logout`;

  const req_options: RequestInit = {
    method: "POST",
    headers: server_headers,
    credentials: "include",
  };

  try {
    // TODO - better error handling
    await rawRequest<void>(logout_url, req_options);
  } catch (err) {
    console.warn("Logout failed:", err);
  }
}

export async function login(
  username?: string,
  password?: string,
): Promise<{
  serverInfo: qcpTypes.ServerInfo;
  userInfo?: qcpTypes.UserInfo;
}> {
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
    const serverInfo = await rawMakeRequest<qcpTypes.ServerInfo>(
      "GET",
      "api/v1/information",
    );
    return { serverInfo, userInfo };
  }

  // else "login" anonymously
  const serverInfo = await rawMakeRequest<qcpTypes.ServerInfo>(
    "GET",
    "api/v1/information",
  );
  return { serverInfo, userInfo: undefined };
}
