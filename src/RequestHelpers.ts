import * as qcpExceptions from "./Exceptions";
import * as qcpTypes from "./PortalTypes"
import {server_address, server_headers} from "./request_config";

function objectToQueryParams(obj: Record<string, unknown>): string {
    const params = new URLSearchParams();

    Object.entries(obj).forEach(([key, value]) => {
        if (Array.isArray(value)) {
            value.forEach((v) => {
                if (v !== undefined && v !== null) {
                    params.append(key, String(v));
                }
            });
        } else if (value !== undefined && value !== null) {
            params.append(key, String(value));
        }
    });

    return params.toString();
}



export async function rawRequest<T>(full_url: string, req_options: RequestInit): Promise<T> {
    // Sends a request to the given URL and options, handles errors, and returns the results
    // Handles appropriate errors. Will throw:
    //     AuthenticationError if the user is not logged in and needs to be
    //     AuthorizationError if the user is logged in, but can't access the resource
    //     Error on other errors

    const response = await fetch(full_url, req_options)
    const content_type = response.headers.get("Content-Type");
    const is_json_response = content_type?.includes("application/json")

    if (response.ok) {
        if (!is_json_response) {
            throw new Error(`Received ok response but not JSON? - (${response.status}): ${response.statusText}`);
        }
        return await response.json() as T;
    } else {
        // error was sent. If it's JSON, there should be a 'msg' field with a description
        if (is_json_response) {
            const rjson = await response.json();
            const message = ('msg' in rjson) ? rjson.msg as string : '(no message)';

            if (response.status === 401) {
                // 401 will mean the user is no longer authenticated
                throw new qcpExceptions.AuthenticationError(`API request failed (401 ${response.statusText}) - ${message}`);
            } else if (response.status === 403) {
                throw new qcpExceptions.AuthorizationError(`API request failed (403 ${response.statusText}) - ${message}`);
            } else {
                throw new Error(`API request failed (${response.status}): ${response.statusText} - ${rjson.msg}`);
            }
        } else {
            throw new Error(`API request failed with unexpected response type (${response.status}): ${response.statusText}`);
        }
    }
}

export async function rawMakeRequest<T>(method: string, endpoint: string, body?: object, url_params?: Record<string, string>): Promise<T> {

    const req_options: RequestInit = {
        method,
        headers: server_headers,
        credentials: "include",
        body: body ? JSON.stringify(body) : undefined,
    };

    // Put URL params at the end of the url
    const full_url = url_params
        ? `${server_address}/${endpoint}?${objectToQueryParams(url_params)}`
        : `${server_address}/${endpoint}`;

    return rawRequest<T>(full_url, req_options);
}


export async function ping(): Promise<qcpTypes.PingResults> {
    try {
        return await rawMakeRequest<qcpTypes.PingResults>(
            "get",
            `api/v1/ping`,
        );
    }
    catch {
        return {success: false, authorized: false, user_info: undefined};
    }
}

export async function serverInfo(): Promise<qcpTypes.ServerInfo | undefined> {
    try {
        return await rawMakeRequest <qcpTypes.ServerInfo>("get", `api/v1/information`);
    }
    catch (e) {
        if (e instanceof qcpExceptions.AuthenticationError) {
            return undefined;
        }
        else {
            throw e;
        }
    }
}

export async function checkLoginState(): Promise<qcpTypes.ConnectionState> {
    const pr = await ping();
    if (!pr.success) {
        // Trouble connecting to the server
        return {connected: false, authorized: false, userInfo: undefined};
    }

    return { connected: true, authorized: pr.authorized, userInfo: pr.user_info }
}
