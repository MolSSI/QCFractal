import { RecordType } from "./record_types";

export type ServerInfo = {
  name: string;
  version: string;
  api_limits: {
    get_records: number;
    add_records: number;
    get_dataset_entries: number;
    get_molecules: number;
    get_managers: number;
  };
};

export type UserInfo = {
  id?: number;
  user_id?: number;
  username: string;
  groups: string[];
  role: string;
  auth_type: string;
  enabled: boolean;
  fullname?: string;
  organization?: string;
  email?: string;
};

export type PingResults = {
  success: boolean;
  authorized: boolean;
  user_info?: UserInfo;
};

export type WaitingReason = {
  reason: string;
  details: Record<string, string>;
};

export type PriorityEnum = 0 | 1 | 2;

export type ProjectAddBody = {
  name: string;
  description: string;
  tagline: string;
  tags: string[];
  default_compute_tag: string;
  default_compute_priority: PriorityEnum;
  extras: object;
  existing_ok?: boolean;
};

export type ProjectListEntry = {
  id: number;
  project_name: string;
  tagline: string;
  tags: string[];
  record_count: number;
  dataset_count: number;
  owner_user: string;
};

export type Project = {
  id: string;
  name: string;
  description: string;
  tagline: string;
  tags: string[];
  record_count: number;
  dataset_count: number;
  owner_user: string;
};

export type ProjectDatasetMetadata = {
  dataset_id: number;
  dataset_type: RecordType;
  name: string;
  description: string;
  tagline: string;
  tags?: string[];
};

export type ProjectRecordMetadata = {
  record_id: number;
  record_type: RecordType;
  status: string;
  name: string;
  description: string;
  tags?: string[];
};

export type Manager = {
  id: number;
  manager_version: string;
  name: string;
  cluster: string;
  hostname: string;
  username: string;
  tags: string[];
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
  symbols: string[];
  geometry: number[];
  connectivity: Array<[number, number, number]>;
  real: boolean[];

  identifiers: MoleculeIdentifiers;
};

export type ServiceDependency = {
  record_id: number;
  extras: object;
};

export type RecordService = {
  id: number;
  record_id: string;

  compute_tag: string;
  compute_priority: number;
  find_existing: boolean;

  service_state: object | null;
  dependencies: Array<ServiceDependency>;
};

export type RecordTask = {
  id: number;
  record_id: string;

  function: string | null;

  compute_tag: string;
  compute_priority: number;
  required_program: string[];
};

export type RecordStatus =
  | "complete"
  | "waiting"
  | "running"
  | "error"
  | "cancelled"
  | "deleted"
  | "invalid";

export type BaseRecord = {
  id: number;
  record_type: RecordType;
  is_service: boolean;
  status: RecordStatus;
  created_on: string;
  modified_on: string;
  name?: string;
  description?: string;
  tags: string[];
  manager_name?: string;
  owner_user?: string;
  owner_group?: string | null;
  compute_history?: ComputeHistory[];
  task?: RecordTask | null;
  service?: RecordService | null;
  comments?: RecordComment[];
  native_files?: Record<string, any>;
  extras: Record<string, any>;
  properties: Record<string, any>;
};

export type RecordComment = {
  id: number;
  record_id: number;
  username: string;
  timestamp: string;
  comment: string;
};

export type QCSpecification = {
  program: string;
  driver: string;
  method: string;
  basis: string | null;
  keywords: Record<string, any>;
  protocols: Record<string, any>;
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

export type UserPreferences = Record<string, any>;

export type DatasetQueryModel = {
  dataset_type: string;
  dataset_name: string;
  include?: string[];
  exclude?: string[];
};

export type ProjectLinkDatasetBody = {
  dataset_id: number;
};

export type DatasetStatus = Record<string, Record<RecordStatus, number>>;

export type DatasetListEntry = {
  id: number;
  dataset_type: RecordType;
  dataset_name: string;
  tagline: string;
  description: string;
  record_count: number;
};

export type Dataset = {
  id: number;
  dataset_type: RecordType;
  name: string;
  description: string;
  tagline: string;
  tags: string[];
  group: string;
  visibility: boolean;
  default_compute_tag: string;
  default_compute_priority: number;
  provenance: Record<string, any>;
  extras: Record<string, any>;

  // These might be present depending on the include/exclude
  specifications?: Record<string, any>;
  entries?: Record<string, any>;
};

export type ExternalFileStatus = "available" | "processing";

export type Attachment = {
  id: number;
  file_type: string;

  created_on: string;
  status: ExternalFileStatus;

  file_name: string;
  description: string | undefined;
  provenance: Record<string, any>;
  sha256sum: string;
  file_size: number;
};

export type DatasetAttachmentType = "other" | "notebook" | "view";

export type DatasetAttachment = Attachment & {
  attachment_type: DatasetAttachmentType;
};

export type ProjectAttachmentType = "other" | "notebook";

export type ProjectAttachment = Attachment & {
  attachment_type: ProjectAttachmentType;
  tags: string[];
};