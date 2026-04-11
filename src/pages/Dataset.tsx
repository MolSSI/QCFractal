import React, { useEffect } from "react";
import { usePortalClient } from "../PortalClient.tsx";
import * as qcpTypes from "../PortalTypes";
import { useLocation, useParams } from "react-router-dom";
import {
  Box,
  Chip,
  Grid,
  IconButton,
  Paper,
  Tab,
  Tabs,
  Tooltip,
  Typography,
} from "@mui/material";
import RefreshIcon from "@mui/icons-material/Refresh";
import LoadingIndicator from "../components/LoadingIndicator";
import ErrorIndicator from "../components/ErrorIndicator";
import { useQuery, useQueryClient } from "@tanstack/react-query";
import DatasetStatusTable from "../components/dataset_components/DatasetStatusTable";
import DatasetActions from "../components/dataset_components/DatasetActions";
import DatasetSpecificationTable from "../components/dataset_components/DatasetSpecificationTable";
import DatasetEntryTable from "../components/dataset_components/DatasetEntryTable";
import DatasetRecords from "../components/dataset_components/DatasetRecords";
import { asRecord } from "../Utils.ts";
import {
  areDatasetViewStatesEqual,
  createDefaultDatasetViewState,
  createSavedDatasetPageState,
  DatasetLocationState,
  DatasetRecordViewState,
  DatasetViewState,
  getSavedDatasetViewState,
} from "../components/dataset_components/DatasetViewState.tsx";
import { FavoriteButton } from "../components/FavoriteButton.tsx";

const RECORDS_TAB_INDEX = 2;

function TabPanel(props: {
  children?: React.ReactNode;
  index: number;
  value: number;
}) {
  const { children, value, index, ...other } = props;
  return (
    <div
      role="tabpanel"
      hidden={value !== index}
      id={`tabpanel-${index}`}
      aria-labelledby={`tab-${index}`}
      {...other}
    >
      {value === index && <Box p={2}>{children}</Box>}
    </div>
  );
}

export default function Dataset() {
  const { datasetId } = useParams();
  const location = useLocation();
  const { makeRequest } = usePortalClient();
  const queryClient = useQueryClient();

  const restoredViewState = React.useMemo(
    () =>
      getSavedDatasetViewState(location.state, datasetId) ||
      createDefaultDatasetViewState(),
    [datasetId, location.state],
  );

  const [storedDatasetViewState, setStoredDatasetViewState] = React.useState<{
    datasetId?: string;
    viewState: DatasetViewState;
  }>(() => ({
    datasetId,
    viewState: restoredViewState,
  }));

  const viewState =
    storedDatasetViewState.datasetId === datasetId
      ? storedDatasetViewState.viewState
      : restoredViewState;

  const setViewState = (
    update:
      | DatasetViewState
      | ((currentViewState: DatasetViewState) => DatasetViewState),
  ) => {
    setStoredDatasetViewState((currentStoredState) => {
      const currentViewState =
        currentStoredState.datasetId === datasetId
          ? currentStoredState.viewState
          : restoredViewState;
      const nextViewState =
        typeof update === "function" ? update(currentViewState) : update;

      return {
        datasetId,
        viewState: nextViewState,
      };
    });
  };

  useEffect(() => {
    if (!datasetId) {
      return;
    }

    const historyState = asRecord(window.history.state) || {};
    const userState = asRecord(historyState.usr) || {};
    const currentSavedState = getSavedDatasetViewState(userState, datasetId);

    if (
      currentSavedState &&
      areDatasetViewStatesEqual(currentSavedState, viewState)
    ) {
      return;
    }

    window.history.replaceState(
      {
        ...historyState,
        usr: {
          ...userState,
          datasetPageState: createSavedDatasetPageState(datasetId, viewState),
        } satisfies DatasetLocationState,
      },
      "",
      `${location.pathname}${location.search}${location.hash}`,
    );
  }, [datasetId, location.hash, location.pathname, location.search, viewState]);

  const handleTabChange = (_event: React.SyntheticEvent, newValue: number) => {
    setViewState((currentViewState) => ({
      ...currentViewState,
      tabValue: newValue,
    }));
  };

  const updateRecordView = (updates: Partial<DatasetRecordViewState>) => {
    setViewState((currentViewState) => ({
      ...currentViewState,
      recordView: {
        ...currentViewState.recordView,
        ...updates,
      },
    }));
  };

  const handleRowsPerPageChange = (rowsPerPage: number) => {
    updateRecordView({
      rowsPerPage,
      page: 0,
    });
  };

  const handleEntryFilterChange = (entryFilter: string) => {
    updateRecordView({
      entryFilter,
      page: 0,
    });
  };

  const handleSpecFilterChange = (specFilter: string) => {
    updateRecordView({
      specFilter,
      page: 0,
    });
  };

  const handleStatusFilterChange = (
    statusFilter: "all" | qcpTypes.RecordStatus,
  ) => {
    updateRecordView({
      statusFilter,
      page: 0,
    });
  };

  const handleStatusTableSelect = (
    specificationName: string,
    status: qcpTypes.RecordStatus,
  ) => {
    setViewState((currentViewState) => ({
      tabValue: RECORDS_TAB_INDEX,
      recordView: {
        ...currentViewState.recordView,
        page: 0,
        entryFilter: "",
        specFilter: specificationName,
        statusFilter: status,
      },
    }));
  };

  const handleRefresh = () => {
    if (!datasetId) {
      return;
    }

    queryClient.invalidateQueries({
      queryKey: ["dataset", datasetId],
    });

    if (!datasetData?.dataset_type) {
      return;
    }

    const datasetScopedKeys = [
      ["datasetStatus", datasetData.dataset_type, datasetId],
      ["datasetSpecifications", datasetData.dataset_type, datasetId],
      ["datasetEntryNames", datasetData.dataset_type, datasetId],
      ["datasetRecordCount", datasetData.dataset_type, datasetId],
      ["datasetRecordDiscovery", datasetData.dataset_type, datasetId],
    ] as const;

    datasetScopedKeys.forEach((queryKey) => {
      queryClient.invalidateQueries({ queryKey });
    });
  };

  const {
    status: datasetStatus,
    data: datasetData,
    error: datasetError,
  } = useQuery({
    queryKey: ["dataset", datasetId],
    queryFn: () =>
      makeRequest<qcpTypes.Dataset>("GET", `api/v1/datasets/${datasetId}`),
    enabled: !!datasetId,
  });

  useEffect(() => {
    if (datasetData) {
      document.title = `Dataset ${datasetData.id}: ${datasetData.name}`;
    } else {
      document.title = `Dataset ${datasetId}`;
    }
  }, [datasetData, datasetId]);

  const { data: statusData } = useQuery({
    queryKey: ["datasetStatus", datasetData?.dataset_type, datasetId],
    queryFn: () =>
      makeRequest<qcpTypes.DatasetStatus>(
        "GET",
        `api/v1/datasets/${datasetData?.dataset_type}/${datasetId}/status`,
      ),
    enabled: !!datasetId && !!datasetData?.dataset_type,
  });

  const { data: specificationsData } = useQuery({
    queryKey: ["datasetSpecifications", datasetData?.dataset_type, datasetId],
    queryFn: () =>
      makeRequest<
        Record<string, { specification: qcpTypes.DatasetSpecificationData }>
      >(
        "GET",
        `api/v1/datasets/${datasetData?.dataset_type}/${datasetId}/specifications`,
      ),
    enabled: !!datasetId && !!datasetData?.dataset_type,
  });

  const { data: entryNamesData } = useQuery({
    queryKey: ["datasetEntryNames", datasetData?.dataset_type, datasetId],
    queryFn: () =>
      makeRequest<string[]>(
        "GET",
        `api/v1/datasets/${datasetData?.dataset_type}/${datasetId}/entry_names`,
      ),
    enabled: !!datasetId && !!datasetData?.dataset_type,
  });

  const { data: recordCountData } = useQuery({
    queryKey: ["datasetRecordCount", datasetData?.dataset_type, datasetId],
    queryFn: () =>
      makeRequest<number>(
        "GET",
        `api/v1/datasets/${datasetData?.dataset_type}/${datasetId}/record_count`,
      ),
    enabled: !!datasetId && !!datasetData?.dataset_type,
  });

  if (!datasetId) {
    return <ErrorIndicator fullPage message="Missing dataset ID" />;
  }

  return (
    <>
      {datasetStatus === "pending" && <LoadingIndicator fullPage />}

      {datasetStatus === "error" && (
        <ErrorIndicator fullPage message={datasetError.message} />
      )}

      {datasetStatus === "success" && datasetData && (
        <Grid container spacing={2} width="100%">
          <Grid size={12}>
            <Box sx={{ display: "flex", alignItems: "center" }}>
              <FavoriteButton
                preferencesKey="favorite_datasets"
                objectId={parseInt(datasetId)}
              />
              <Box>
                <Typography variant="h4" fontWeight="bold">
                  <Typography
                    variant="h5"
                    fontWeight="bold"
                    component={"span"}
                    pr={2}
                  >
                    [{datasetData.id}]
                  </Typography>
                  <Chip
                    label={datasetData.dataset_type}
                    color="primary"
                    variant="outlined"
                    sx={{ mr: 2, verticalAlign: "middle" }}
                  />
                  {datasetData.name}
                </Typography>
                <Typography
                  variant="subtitle1"
                  sx={{ color: "text.secondary" }}
                >
                  {datasetData.tagline}
                </Typography>
              </Box>
              <Box sx={{ ml: "auto" }}>
                <Tooltip title="Refresh dataset information">
                  <IconButton onClick={handleRefresh} color="primary">
                    <RefreshIcon />
                  </IconButton>
                </Tooltip>
              </Box>
            </Box>
          </Grid>
          {/* Summary stats: for example, Specifications, Entries */}
          <Grid size={2}>
            <Paper elevation={3}>
              <Box p={1}>
                <Typography variant="body1" fontWeight="bold">
                  {entryNamesData ? entryNamesData.length : 0} Entries
                </Typography>
              </Box>
            </Paper>
          </Grid>
          <Grid size={2}>
            <Paper elevation={3}>
              <Box p={1}>
                <Typography variant="body1" fontWeight="bold">
                  {specificationsData
                    ? Object.keys(specificationsData).length
                    : 0}{" "}
                  Specifications
                </Typography>
              </Box>
            </Paper>
          </Grid>
          <Grid size={2}>
            <Paper elevation={3}>
              <Box p={1}>
                <Typography variant="body1" fontWeight="bold">
                  {recordCountData ? recordCountData : 0} Records
                </Typography>
              </Box>
            </Paper>
          </Grid>

          {/* Description & Metadata section */}
          <Grid size={12}>
            <Paper elevation={2}>
              <Box p={2}>
                <Typography variant="h6" fontWeight="bold" gutterBottom>
                  Description
                </Typography>
                <Typography variant="body1" paragraph>
                  {datasetData.description}
                </Typography>

                <Typography variant="h6" fontWeight="bold" gutterBottom>
                  Tags
                </Typography>
                <Box sx={{ display: "flex", gap: 1, flexWrap: "wrap", mb: 2 }}>
                  {datasetData.tags?.map((tag, idx) => (
                    <Chip key={idx} label={tag} variant="outlined" />
                  ))}
                </Box>

                <Box sx={{ mb: 2 }}>
                  <Typography variant="h6" fontWeight="bold" gutterBottom>
                    Group
                  </Typography>
                  <Typography variant="body1">
                    {datasetData.group || "N/A"}
                  </Typography>
                </Box>

                <Box sx={{ mb: 2 }}>
                  <Typography variant="h6" fontWeight="bold" gutterBottom>
                    Default Compute Tag
                  </Typography>
                  <Typography variant="body1">
                    {datasetData.default_compute_tag || "N/A"}
                  </Typography>
                </Box>

                <Box sx={{ mb: 2 }}>
                  <Typography variant="h6" fontWeight="bold" gutterBottom>
                    Default Compute Priority
                  </Typography>
                  <Typography variant="body1">
                    {datasetData.default_compute_priority}
                  </Typography>
                </Box>
              </Box>
            </Paper>
          </Grid>

          {/* Status section */}
          <Grid size={6}>
            <Paper elevation={2}>
              <Box p={2} borderBottom={1} borderColor="divider">
                <Typography variant="h6" fontWeight="bold">
                  Status
                </Typography>
              </Box>
              <Box p={2}>
                {!statusData ? (
                  <LoadingIndicator />
                ) : (
                  <DatasetStatusTable
                    statusData={statusData}
                    onSelectStatus={handleStatusTableSelect}
                  />
                )}
              </Box>
            </Paper>
          </Grid>

          <Grid size={6}>
            <Paper elevation={2}>
              <Box p={2} borderBottom={1} borderColor="divider">
                <Typography variant="h6" fontWeight="bold">
                  Actions
                </Typography>
              </Box>
              <DatasetActions />
            </Paper>
          </Grid>

          {/* Tabs for Specifications, Entries */}
          <Grid size={12} mt={2}>
            <Paper elevation={2}>
              <Tabs
                value={viewState.tabValue}
                onChange={handleTabChange}
                aria-label="dataset sections tabs"
                variant="fullWidth"
              >
                <Tab
                  label="Specifications"
                  id="tab-0"
                  aria-controls="tabpanel-0"
                />
                <Tab label="Entries" id="tab-1" aria-controls="tabpanel-1" />
                <Tab label="Records" id="tab-2" aria-controls="tabpanel-2" />
              </Tabs>

              {/* Tab 0: Specifications */}
              <TabPanel value={viewState.tabValue} index={0}>
                {!specificationsData ? (
                  <LoadingIndicator />
                ) : (
                  <DatasetSpecificationTable
                    specificationsData={specificationsData}
                    datasetType={datasetData.dataset_type}
                  />
                )}
              </TabPanel>

              {/* Tab 1: Entries */}
              <TabPanel value={viewState.tabValue} index={1}>
                {!entryNamesData ? (
                  <LoadingIndicator />
                ) : (
                  <DatasetEntryTable
                    entryNames={entryNamesData}
                    datasetType={datasetData.dataset_type}
                    datasetId={datasetId}
                  />
                )}
              </TabPanel>

              {/* Tab 2: Records */}
              <TabPanel value={viewState.tabValue} index={2}>
                {!entryNamesData || !specificationsData || !statusData ? (
                  <LoadingIndicator />
                ) : (
                  <DatasetRecords
                    datasetId={datasetId}
                    datasetType={datasetData.dataset_type}
                    datasetStatus={statusData}
                    specifications={Object.keys(specificationsData)}
                    entryNames={entryNamesData}
                    totalRecords={recordCountData}
                    page={viewState.recordView.page}
                    rowsPerPage={viewState.recordView.rowsPerPage}
                    entryFilter={viewState.recordView.entryFilter}
                    specFilter={viewState.recordView.specFilter}
                    statusFilter={viewState.recordView.statusFilter}
                    onPageChange={(page) => updateRecordView({ page })}
                    onRowsPerPageChange={handleRowsPerPageChange}
                    onEntryFilterChange={handleEntryFilterChange}
                    onSpecFilterChange={handleSpecFilterChange}
                    onStatusFilterChange={handleStatusFilterChange}
                  />
                )}
              </TabPanel>
            </Paper>
          </Grid>
        </Grid>
      )}
    </>
  );
}
