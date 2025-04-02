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

export type WaitingReason = {
    reason: string,
    details: Record<string, string>
};

export type Project = {
    id: number,
    name: string,
    tagline: string,
    description: string,
    tags: string[],
};

export type CalculationRecord = {
    record_id: number,
    record_type: string,
    status: string,
};
