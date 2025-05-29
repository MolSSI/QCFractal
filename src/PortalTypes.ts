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
  owner_user: string | null;
};

export type ProjectDatasetMetadata = {
  dataset_id: number;
  dataset_type: string;
  name: string;
};

export type ProjectRecordMetadata = {
  record_id: number;
  name: string;
  status: string;
};

export type Manager = {
  id: number;
  manager_version: string;
  name: string;
  cluster: string;
  hostname: string;
  username: string;
  tags: Array<string>;
  programs: Record<string, string>;

  status: string;
  created_on: string;
  modified_on: string;

  claimed: number;
  successes: number;
  failures: number;
  rejected: number;

  total_cpu_hours: number;
  active_tasks: number;
  active_cores: number;
  active_memory: number;
};

export type QueryProjModelBase = {
  limit?: number;
  cursor?: number;
  include?: string[];
  exclude?: string[];
};

export type ManagerQueryFilters = QueryProjModelBase & {
  manager_id?: number[];
  name?: string[];
  cluster?: string[];
  hostname?: string[];
  status?: string[];
  modified_before?: string;
  modified_after?: string;
};

export type ActiveManagerQuery = {
  compute_tag: string[];
  programs: Record<string, string[]>;
};

export type MoleculeIdentifiers = {
  molecule_hash: string;
  molecular_formula: string;
};

export type Molecule = {
  id: number;
  name?: string;
  symbols: Array<string>;
  geometry: Array<number>;
  connectivity: Array<[number, number, number]>;
  real: Array<boolean>;

  identifiers: MoleculeIdentifiers;
};

export type RecordData = {
  name?: string;
  description?: string;
  manager_name: string;
  status: string;
  id: number;
  record_type: string;
  tags: Array<string>;
  is_service: boolean;
  service: Record<string, any>;
  task: Record<string, any>;
  created_on: string;
  modified_on: string;
  owner_group: string | null;
  specification: {
    driver: string;
    basis: string;
    method: string;
    program: string;
    keywords: Record<string, any>;
  };
  properties: Record<string, any>;
};

export type Task = {
  record_id: number;
  id: number;
  function: string;
  tag: string;
  priority: number;
  required_program: Array<string>;
};

export type Service = {
  record_id: number;
  find_existing: boolean;
  id: number;
  service_state: string | null;
  dependencies: Array<any>;
  tag: string;
  priority: number;
};

export type ComputeHistory = {
  id: number;
  modified_on: string;
  record_id: number;
  manager_name: string;
  status: string;
  provenance: {
    creator: string;
    version: string;
    routine: string;
    username: string;
    cpu: string;
    hostname: string;
    qcengine_version: string;
    wall_time: number;
  };
};
