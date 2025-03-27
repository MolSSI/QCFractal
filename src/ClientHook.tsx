import { useEffect, useState } from "react";
import { PortalClient } from "./services/client.ts"
import { ServerInfo } from "./services/types.ts";

const client = new PortalClient("http://localhost:7900");

export type ClientStatus = {
    connected: boolean,
    username?: string,
    serverInfo?: ServerInfo
}

export function useClient() {
    const [clientStatus, setClientStatus] = useState<ClientStatus>({
        username: "",
        connected: false,
        serverInfo: undefined
    });

    useEffect(() => {
        setClientStatus({
            username: client.username,
            connected: client.isConnected,
            serverInfo: client.serverInfo
        })
    }, []);

    async function login(username: string, password: string) {
        try {
            await client.connect(username, password);
        }
        finally {
            console.log("Connected?");
            setClientStatus({
                username: client.username,
                connected: client.isConnected,
                serverInfo: client.serverInfo
            })
        }
    }

    async function logout() {
        try {
            await client.disconnect(true);
        } catch (error) {
            console.error(error);
        } finally {
            setClientStatus({
                username: "",
                connected: false,
                serverInfo: undefined
            })
        }
    }

    return { clientStatus, login, logout };
}