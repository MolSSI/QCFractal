import {createContext} from "react";
import * as qcpTypes from "./PortalTypes";

export type RequestReturnType<T> = {
    data?: T,
    error?: string
}
type ClientContextType = {
    connectionState: qcpTypes.ConnectionState;
    serverInfo: qcpTypes.ServerInfo;

    login: (username: string, password: string) => Promise<void>;
    logout: (force: boolean) => Promise<void>;

    makeRequest: <T> (method: string, endpoint: string, body?: object, url_params?: Record<string, string>)
        => Promise<RequestReturnType<T>>;
};

// Create context with default `undefined` to force usage inside a Provider
export const PortalClientContext = createContext<ClientContextType | undefined>(undefined);