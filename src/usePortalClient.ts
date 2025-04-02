import {useContext} from "react";
import {PortalClientContext} from "./PortalClientContext";

// Hook for authentication-related functionality
export function usePortalClientAuth() {
    const context = useContext(PortalClientContext);
    if (!context) {
        throw new Error("usePortalClientAuth must be used within a PortalClientProvider");
    }
    const {connectionState, serverInfo, login, logout} = context;
    return {connectionState, serverInfo, login, logout};
}

// Hook for general request functionality
export function usePortalClientRequest() {

    const context = useContext(PortalClientContext);
    if (!context) {
        throw new Error("usePortalClientRequest must be used within a PortalClientProvider");
    }

    const {connectionState, makeRequest} = context;
    return {connectionState, makeRequest};
}