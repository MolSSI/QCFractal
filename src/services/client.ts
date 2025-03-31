import * as qcpTypes from './types.ts'
import * as qcpExceptions from "./exceptions"

function objectToQueryParams(obj: Record<string, any>): string {
    const params = new URLSearchParams();
    for (const [key, value] of Object.entries(obj)) {
        if (value !== undefined && value !== null) {
            params.append(key, String(value));
        }
    }
    return params.toString();
}

export class PortalClient {
    private readonly _baseUrl: string;
    private readonly _headers: Record<string, string>

    private _serverInfo: qcpTypes.ServerInfo = { name: "(unknown)", version: "(unknown)" };
    private _connectionState: qcpTypes.ConnectionState = { connected: false }

    constructor(baseUrl: string) {
        this._baseUrl = baseUrl;
        this._headers = {
            'Content-Type': 'application/json',
            'Accept': 'application/json',
        };
    }

    private async rawRequest<T>(full_url: string, req_options: RequestInit): Promise<T> {
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

                if (response.status == 401) {
                    // 401 will mean the user is no longer authenticated
                    await this.disconnect(true)
                    throw new qcpExceptions.AuthenticationError(`API request failed (401 ${response.statusText}) - ${message}`);
                } else if (response.status == 403) {
                    throw new qcpExceptions.AuthorizationError(`API request failed (403 ${response.statusText}) - ${message}`);
                } else {
                    throw new Error(`API request failed (${response.status}): ${response.statusText} - ${rjson.msg}`);
                }
            } else {
                throw new Error(`API request failed with unexpected response type (${response.status}): ${response.statusText}`);
            }
        }
    }

    async makeRequest<T>(method: string, endpoint: string, body?: object, url_params?: Record<string, string>): Promise<T> {

        const req_options: RequestInit = {
            method,
            headers: this._headers,
            credentials: "include",
            body: body ? JSON.stringify(body) : undefined,
        };

        // Put URL params at the end of the url
        let full_url = `${this._baseUrl}/${endpoint}`;
        if (url_params) {
            const url_params_str: string = objectToQueryParams(url_params);
            full_url += `?${url_params_str}`;
        }

        return this.rawRequest<T>(full_url, req_options);
    }

    async connect(username?: string, password?: string): Promise<void> {

        if (this._connectionState.connected) { await this.disconnect(false); }

        if (password && !username) { throw new Error("Username is required if password is specified"); }
        if (username && !password) { throw new Error("Password is required if username is specified"); }

        if (username && password) {
            const login_url = `${this._baseUrl}/auth/v1/session_login`;

            const body = {
                "username": username,
                "password": password,
            };

            const req_options: RequestInit = {
                method: "POST",
                headers: this._headers,
                credentials: "include",
                body: JSON.stringify(body)
            };

            // throws an exception on error
            const uinfo = await this.rawRequest<qcpTypes.UserInfo>(login_url, req_options);

            this._connectionState.connected = true;
            this._connectionState.userInfo = uinfo;
        }

        this._serverInfo = await this.getServerInfo()
    }

    async disconnect(force: boolean): Promise<void> {
        if (force || this._connectionState.connected) {
            const logout_url: string = `${this._baseUrl}/auth/v1/session_logout`;

            const req_options: RequestInit = {
                method: "POST",
                headers: this._headers,
                credentials: "include",
            };

            try {
                // TODO - better error handling
                await this.rawRequest<void>(logout_url, req_options)
            } catch (err) {
                console.error(err);
            }

            this._connectionState = { connected: false, userInfo: undefined };
            this._serverInfo = { name: "(unknown)", version: "(unknown)" };
        }
    }

    // Some getters
    get isAnonymous(): boolean {
        return this._connectionState.userInfo === undefined;
    }

    get isConnected(): boolean {
        return this._connectionState.connected;
    }

    get isAuthenticated(): boolean {
        return this.isConnected && (! this.isAnonymous)
    }

    get username(): string {
        if (this.isAnonymous) {
            return "(anonymous)";
        }
        else {
            // userInfo is not null because of check above
            return this._connectionState.userInfo!.username;
        }
    }

    get serverInfo(): qcpTypes.ServerInfo {
        return this._serverInfo;
    }

    get connectionState(): qcpTypes.ConnectionState {
        return this._connectionState;
    }

    /////////////////////////////////////////////////////////
    // Wrappers for getting data from the server
    async ping(): Promise<qcpTypes.PingResults> {
        return await this.makeRequest<qcpTypes.PingResults>('GET', 'api/v1/ping');
    }

    async getServerInfo(): Promise<qcpTypes.ServerInfo> {
        // Returns more than what we store, but that's ok
        return await this.makeRequest<qcpTypes.ServerInfo>('GET', 'api/v1/information');
    }

    async getProjectsList(): Promise<qcpTypes.ProjectsList> {
        return await this.makeRequest<qcpTypes.ProjectsList>('GET', 'api/v1/projects');
    }
}

