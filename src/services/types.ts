export type ServerInfo = {
    name: string,
    version: string,
};

export type UserInfo = {
    user_id: number,
    username: string,
    groups: string[],
    role: string,
}

export type ConnectionState = {
    connected: boolean,
    userInfo?: UserInfo,
};

export type PingResults = {
    success: boolean,
    user_id: number,
    username: string,
};

export type ProjectsList = {
    id: number,
    name: string,
    tagline: string,
    tags: string[],
};

