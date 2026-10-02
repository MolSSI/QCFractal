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
    get_error_logs: number;
  };
};

export type UserInfo = {
  id?: number;
  username: string;
  groups: string[];
  role: string;
  auth_type: string;
  enabled: boolean;
  fullname?: string;
  organization?: string;
  email?: string;
};

export type GroupInfo = {
  id?: number;
  groupname: string;
  description?: string;
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

export type ServerStatsRecordCountDetails = Record<
  string,
  Partial<Record<RecordStatus, number>>
>;

export type ServerStatsEntry = {
  date: string;
  record_count: number;
  cpu_hours: number;
  record_count_details: ServerStatsRecordCountDetails;
  database_size: number;
  timestamp: string;
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

// Note the asymmetry, the same one the dataset defaults have: the server
// returns these as tag/priority, while the PATCH body takes compute_tag/
// compute_priority. Newer servers send the compute_ spelling, so read both.
type ComputeQueueFields = {
  tag?: string;
  priority?: number;
  compute_tag?: string;
  compute_priority?: number;
};

export type RecordService = ComputeQueueFields & {
  id: number;
  record_id: string;

  find_existing: boolean;

  service_state: object | null;
  dependencies: Array<ServiceDependency>;
};

export type RecordTask = ComputeQueueFields & {
  id: number;
  record_id: string;

  function: string | null;

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

// Body for PATCH api/v1/records. Single-record actions use a one-element
// record_ids array; the server resolves the record type from the id.
// Send one concern per request - the returned metadata only describes the
// last field the backend handled.
export type RecordModifyBody = {
  record_ids: number[];
  status?: RecordStatus;
  compute_priority?: PriorityEnum;
  compute_tag?: string;
  comment?: string;
};

// Body for POST api/v1/records/revert. revert_status is the status being
// *undone* - so uncancelling sends "cancelled". The record goes back to
// whatever status it held before, not necessarily waiting.
export type RecordRevertBody = {
  record_ids: number[];
  revert_status: RecordStatus;
};

// Body for POST api/v1/records/bulkDelete. Deletion never goes through the
// PATCH endpoint - sending status: "deleted" there 500s.
export type RecordDeleteBody = {
  record_ids: number[];
  soft_delete: boolean;
  delete_children: boolean;
};

// Returned by the bulk record modification endpoints. A rejected action still
// comes back 200, with an empty updated_idx and the reason in errors.
export type UpdateMetadata = {
  updated_idx: number[];
  n_children_updated: number;
  errors: [number, string][];
  error_description: string | null;
};

// Same shape as UpdateMetadata, returned by the bulkDelete endpoints.
export type DeleteMetadata = {
  deleted_idx: number[];
  n_children_deleted: number;
  errors: [number, string][];
  error_description: string | null;
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

export type Session = {
  id: number;
  ip_address: string;
  user_agent: string;
  created_on: string;
  last_used: string;
};

export type APIToken = {
  id: number;
  user_id: number;
  token_prefix: string;
  name: string;
  scope?: string;
  created_at: string;
  expires_at?: string | null;
  last_used_at?: string | null;
};

export type APITokenCreateBody = {
  name: string;
  scope?: string;
  expires_at?: string | null;
};

// Name is the only mutable property of a token
export type APITokenModifyBody = {
  name: string;
};

// The plaintext token is only ever present here, in the create response
export type NewAPIToken = {
  token: string;
  info: APIToken;
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
  name: string | null;
  description: string | null;
  tagline: string | null;
  tags: string[] | null;
};

export type ProjectUnlinkLinkDatasetBody = {
  dataset_ids: number[];
  delete_datasets: boolean;
  delete_dataset_records: boolean;
};

export type DatasetStatus = Record<string, Record<RecordStatus, number>>;

// Response row from POST api/v1/datasets/queryrecords.
export type DatasetRecordLocation = {
  record_id: number;
  dataset_id: number;
  dataset_type: RecordType;
  dataset_name: string;
  entry_name: string;
  specification_name: string;
};

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
  visibility: boolean;
  // Note the asymmetry: GET returns these as default_tag/default_priority,
  // while the PATCH body below takes default_compute_tag/_priority
  default_tag: string;
  default_priority: PriorityEnum;
  provenance: Record<string, any>;
  extras: Record<string, any>;

  // These might be present depending on the include/exclude
  specifications?: Record<string, any>;
  entries?: Record<string, any>;
};

// Body for PATCH api/v1/datasets/{dataset_type}/{dataset_id}.
// The backend requires the full metadata object (no partial updates).
export type DatasetModifyMetadata = {
  name: string;
  description: string;
  tagline: string;
  tags: string[];
  provenance: Record<string, unknown>;
  extras: Record<string, unknown>;
  default_compute_tag: string;
  default_compute_priority: PriorityEnum;
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

export type InternalJobStatusEnum =
  | "complete"
  | "waiting"
  | "running"
  | "error"
  | "cancelled";

export type InternalJob = {
  id: number;
  name: string;
  status: InternalJobStatusEnum;

  added_date: string;
  scheduled_date: string;
  started_date: string | null;
  last_updated: string | null;
  ended_date: string | null;

  runner_hostname: string | null;
  runner_uuid: string | null;
  repeat_delay: number | null;
  serial_group: string | null;

  progress: number;
  progress_description: string | null;
  function: string;
  kwargs: Record<string, any>;
  after_function: string | null;
  after_function_kwargs: Record<string, any> | null;
  result: any;
  user: string | null;
};

export type ServerErrorLog = {
  id: number;
  error_date: string;
  qcfractal_version: string;
  error_text: string;
  user: string | null;
  request_path: string | null;
  request_headers: Record<string, string> | null;
  request_body: string | null;
};

export type ServerErrorLogQueryFilters = {
  error_id?: number[];
  user?: (string | number)[];
  before?: string;
  after?: string;
  limit?: number;
  cursor?: number;
};
