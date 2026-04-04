import * as qcpTypes from "../../PortalTypes.ts";
import { asRecord } from "../../Utils.ts";

export type DatasetRecordViewState = {
  page: number;
  rowsPerPage: number;
  entryFilter: string;
  specFilter: string;
  statusFilter: "all" | qcpTypes.RecordStatus;
};

export type DatasetViewState = {
  tabValue: number;
  recordView: DatasetRecordViewState;
};

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

export function createDefaultDatasetViewState(): DatasetViewState {
  return {
    tabValue: 0,
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

  if (
    !datasetPageState ||
    !recordView ||
    datasetPageState.datasetId !== datasetId
  ) {
    return null;
  }

  return {
    tabValue: datasetPageState.tabValue as number,
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
    recordView: { ...viewState.recordView },
  };
}

export function areDatasetViewStatesEqual(
  left: DatasetViewState,
  right: DatasetViewState,
): boolean {
  return (
    left.tabValue === right.tabValue &&
    left.recordView.page === right.recordView.page &&
    left.recordView.rowsPerPage === right.recordView.rowsPerPage &&
    left.recordView.entryFilter === right.recordView.entryFilter &&
    left.recordView.specFilter === right.recordView.specFilter &&
    left.recordView.statusFilter === right.recordView.statusFilter
  );
}
