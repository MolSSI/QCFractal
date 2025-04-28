export type ServerInfo = {
  name: string;
  version: string;
};

export type UserInfo = {
  user_id: number;
  username: string;
  groups: string[];
  role: string;
  auth_type: string;
  enabled: boolean;
};

export type ConnectionState = {
  connected: boolean;
  authorized: boolean;
  userInfo?: UserInfo;
};

export type PingResults = {
  success: boolean;
  authorized: boolean;
  user_info?: UserInfo;
};

export type ProjectsList = {
  id: number;
  name: string;
  tagline: string;
  tags: string[];
};

export type WaitingReason = {
  reason: string;
  details: Record<string, string>;
};

export type Project = {
  id: number;
  name: string;
  tagline: string;
  description: string;
  tags: string[];
};

export type ProjectDatasetMetadata = {
  dataset_id: number;
}

export type ProjectRecordMetadata = {
  record_id: number;
}

export type CalculationRecord = {
  record_id: number;
  record_type: string;
  status: string;
  name?: string;
  description?: string;
};

export type Manager = {
  id: number;
  manager_version: string;
  name: string;
  cluster: string;
  hostname: string;
  username: string;
  tags: Array<string>;
  programs: Record<string, string>

  status: string;
  created_on: string;
  modified_on: string;
};

export type MoleculeIdentifiers = {
  molecule_hash: string;
  molecular_formula: string;
};

export type Molecule = {
  id: number;
  name?: string;
  symbols: Array<string>;
  geometry: Array<number>
  connectivity: Array<[number, number, number]>;
  real: Array<boolean>;

  identifiers: MoleculeIdentifiers;
};