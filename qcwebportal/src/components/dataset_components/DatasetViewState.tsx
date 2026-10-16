import * as qcpTypes from "../../PortalTypes.ts";
import { asRecord } from "../../Utils.ts";

export type DatasetRecordViewState = {
  page: number;
  rowsPerPage: number;
  entryFilter: string;
  specFilter: string;
  statusFilter: "all" | qcpTypes.RecordStatus;
};

export type DatasetStatusViewState = {
  page: number;
  specFilter: string;
};

export type DatasetViewState = {
  tabValue: number;
  statusView: DatasetStatusViewState;
  recordView: DatasetRecordViewState;
};

export const DATASET_RECORDS_TAB_INDEX = 2;

type SavedDatasetPageState = DatasetViewState & {
  datasetId: string;
};

export type DatasetLocationState = {
  datasetPageState?: SavedDatasetPageState;
} & Record<string, unknown>;

function createDefaultRecordViewState(): DatasetRecordViewState {
  return {
    page: 0,
    rowsPerPage: 10,
    entryFilter: "",
    specFilter: "all",
    statusFilter: "all",
  };
}

function createDefaultStatusViewState(): DatasetStatusViewState {
  return {
    page: 0,
    specFilter: "",
  };
}

export function createDefaultDatasetViewState(): DatasetViewState {
  return {
    tabValue: 0,
    statusView: createDefaultStatusViewState(),
    recordView: createDefaultRecordViewState(),
  };
}

export function getSavedDatasetViewState(
  locationState: unknown,
  datasetId?: string,
): DatasetViewState | null {
  if (!datasetId) {
    return null;
  }

  const stateRecord = asRecord(locationState);
  const datasetPageState = asRecord(stateRecord?.datasetPageState);
  const recordView = asRecord(datasetPageState?.recordView);
  // Saved before the status view existed, so fall back to its defaults
  const statusView = asRecord(datasetPageState?.statusView);

  if (
    !datasetPageState ||
    !recordView ||
    datasetPageState.datasetId !== datasetId
  ) {
    return null;
  }

  return {
    tabValue: datasetPageState.tabValue as number,
    statusView: {
      page: (statusView?.page as number) ?? 0,
      specFilter: (statusView?.specFilter as string) ?? "",
    },
    recordView: {
      page: recordView.page as number,
      rowsPerPage: recordView.rowsPerPage as number,
      entryFilter: recordView.entryFilter as string,
      specFilter: recordView.specFilter as string,
      statusFilter: recordView.statusFilter as "all" | qcpTypes.RecordStatus,
    },
  };
}

export function createSavedDatasetPageState(
  datasetId: string,
  viewState: DatasetViewState,
): SavedDatasetPageState {
  return {
    datasetId,
    tabValue: viewState.tabValue,
    statusView: { ...viewState.statusView },
    recordView: { ...viewState.recordView },
  };
}

export function createDatasetRecordsLocationState(
  datasetId: string,
  entryFilter: string,
  specFilter: string,
): DatasetLocationState {
  return {
    datasetPageState: createSavedDatasetPageState(datasetId, {
      tabValue: DATASET_RECORDS_TAB_INDEX,
      statusView: createDefaultStatusViewState(),
      recordView: {
        ...createDefaultRecordViewState(),
        entryFilter,
        specFilter,
      },
    }),
  };
}

export function areDatasetViewStatesEqual(
  left: DatasetViewState,
  right: DatasetViewState,
): boolean {
  return (
    left.tabValue === right.tabValue &&
    left.statusView.page === right.statusView.page &&
    left.statusView.specFilter === right.statusView.specFilter &&
    left.recordView.page === right.recordView.page &&
    left.recordView.rowsPerPage === right.recordView.rowsPerPage &&
    left.recordView.entryFilter === right.recordView.entryFilter &&
    left.recordView.specFilter === right.recordView.specFilter &&
    left.recordView.statusFilter === right.recordView.statusFilter
  );
}
