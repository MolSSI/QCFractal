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

    private _connected: boolean = false;
    private _username?: string

    private _serverInfo?: qcpTypes.ServerInfo

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

        this._connected = false;
        this._username = undefined;

        if (password && !username) {
            throw new Error("Username is required if password is specified");
        } else if (username && !password) {
            throw new Error("Password is required if username is specified");
        } else if (username && password) {
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
            await this.rawRequest<object>(login_url, req_options);

            this._username = username;
        }

        this._connected = true;
        this._serverInfo = await this.fetchServerInfo()
    }

    async disconnect(force: boolean): Promise<void> {
        if (force || this._connected) {
            const logout_url: string = `${this._baseUrl}/auth/v1/session_logout`;

            const req_options: RequestInit = {
                method: "POST",
                headers: this._headers,
                credentials: "include",
            };

            try {
                await this.rawRequest<void>(logout_url, req_options)
            } catch (err) {
                console.error(err);
            }

            this._connected = false;
            this._username = undefined;
        }
    }

    // Some getters
    get isAnonymous(): boolean {
        return this._username === undefined;
    }

    get isConnected(): boolean {
        return this._connected;
    }

    get isAuthenticated(): boolean {
        return this._connected && (this._username != undefined)
    }

    get username(): string {
        return this._username ? this._username : "(anonymous)";
    }

    get serverInfo(): qcpTypes.ServerInfo {
        if (this._serverInfo == undefined) {
            return {name: "(unknown)", version: "(unknown)"} as qcpTypes.ServerInfo;
        } else {
            return this._serverInfo;
        }
    }

    // Various functions for getting data from the server
    async ping(): Promise<qcpTypes.PingResults> {
        return await this.makeRequest<qcpTypes.PingResults>('GET', 'api/v1/ping');
    }

    async fetchServerInfo(): Promise<qcpTypes.ServerInfo> {
        return this.makeRequest('GET', 'api/v1/information');
    }
}

