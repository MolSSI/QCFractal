import React, { useEffect } from "react";
import { usePortalClient } from "../PortalClient.tsx";
import { usePageTitle } from "../UsePageTitle.ts";
import * as qcpTypes from "../PortalTypes";
import { useLocation, useParams } from "react-router-dom";
import {
  Box,
  Chip,
  Grid,
  IconButton,
  Paper,
  Stack,
  Tab,
  Tabs,
  Tooltip,
  Typography,
} from "@mui/material";
import RefreshIcon from "@mui/icons-material/Refresh";
import LoadingIndicator from "../components/LoadingIndicator";
import ErrorIndicator from "../components/ErrorIndicator";
import { DatasetRelationshipButton } from "../components/DatasetRelationshipDialog";
import { useQuery, useQueryClient } from "@tanstack/react-query";
import DatasetStatusTable from "../components/dataset_components/DatasetStatusTable";
import DatasetActions from "../components/dataset_components/DatasetActions";
import DatasetSpecificationTable from "../components/dataset_components/DatasetSpecificationTable";
import DatasetEntryTable from "../components/dataset_components/DatasetEntryTable";
import DatasetRecords from "../components/dataset_components/DatasetRecords";
import { AttachmentTable } from "../components/AttachmentTable";
import { asRecord } from "../Utils.ts";
import MarkdownContent from "../components/MarkdownContent";
import {
  areDatasetViewStatesEqual,
  createDefaultDatasetViewState,
  DATASET_RECORDS_TAB_INDEX,
  createSavedDatasetPageState,
  DatasetLocationState,
  DatasetRecordViewState,
  DatasetStatusViewState,
  DatasetViewState,
  getSavedDatasetViewState,
} from "../components/dataset_components/DatasetViewState.tsx";
import { FavoriteButton } from "../components/FavoriteButton.tsx";
import {
  EditDatasetMetadataButton,
  EditDatasetNameButton,
} from "../components/dataset_components/EditDatasetDialog.tsx";

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
  const datasetIdNumber = datasetId ? Number.parseInt(datasetId, 10) : null;

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

  const updateStatusView = (updates: Partial<DatasetStatusViewState>) => {
    setViewState((currentViewState) => ({
      ...currentViewState,
      statusView: {
        ...currentViewState.statusView,
        ...updates,
      },
    }));
  };

  const handleStatusSpecFilterChange = (specFilter: string) => {
    updateStatusView({
      specFilter,
      page: 0,
    });
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
      ...currentViewState,
      tabValue: DATASET_RECORDS_TAB_INDEX,
      recordView: {
        ...currentViewState.recordView,
        page: 0,
        entryFilter: "",
        specFilter: specificationName,
        statusFilter: status,
      },
    }));
  };

  const handleRefresh = async () => {
    if (datasetIdNumber === null || Number.isNaN(datasetIdNumber)) {
      return;
    }

    const queryKeys = [["dataset", datasetIdNumber]] as const;

    if (!datasetData?.dataset_type) {
      await Promise.all(
        queryKeys.map((queryKey) =>
          queryClient.invalidateQueries({ queryKey }),
        ),
      );
      return;
    }

    const datasetScopedKeys = [
      ["datasetStatus", datasetData.dataset_type, datasetIdNumber],
      ["datasetSpecifications", datasetData.dataset_type, datasetIdNumber],
      ["datasetEntryNames", datasetData.dataset_type, datasetIdNumber],
      ["datasetRecordCount", datasetData.dataset_type, datasetIdNumber],
      ["datasetRecordDiscovery", datasetData.dataset_type, datasetIdNumber],
      ["datasetAttachments", datasetIdNumber],
    ] as const;

    await Promise.all(
      [...queryKeys, ...datasetScopedKeys].map((queryKey) =>
        queryClient.invalidateQueries({ queryKey }),
      ),
    );
  };

  const {
    status: datasetStatus,
    data: datasetData,
    error: datasetError,
  } = useQuery({
    queryKey: ["dataset", datasetIdNumber],
    queryFn: () =>
      makeRequest<qcpTypes.Dataset>("GET", `api/v1/datasets/${datasetId}`),
    enabled: datasetIdNumber !== null && !Number.isNaN(datasetIdNumber),
  });

  const pageTitle = datasetData
    ? `Dataset ${datasetData.id}: ${datasetData.name}`
    : `Dataset ${datasetId}`;
  usePageTitle(pageTitle);

  const { data: statusData } = useQuery({
    queryKey: ["datasetStatus", datasetData?.dataset_type, datasetIdNumber],
    queryFn: () =>
      makeRequest<qcpTypes.DatasetStatus>(
        "GET",
        `api/v1/datasets/${datasetData?.dataset_type}/${datasetId}/status`,
      ),
    enabled:
      datasetIdNumber !== null &&
      !Number.isNaN(datasetIdNumber) &&
      !!datasetData?.dataset_type,
  });

  const { data: specificationsData } = useQuery({
    queryKey: [
      "datasetSpecifications",
      datasetData?.dataset_type,
      datasetIdNumber,
    ],
    queryFn: () =>
      makeRequest<
        Record<string, { specification: qcpTypes.DatasetSpecificationData }>
      >(
        "GET",
        `api/v1/datasets/${datasetData?.dataset_type}/${datasetId}/specifications`,
      ),
    enabled:
      datasetIdNumber !== null &&
      !Number.isNaN(datasetIdNumber) &&
      !!datasetData?.dataset_type,
  });

  const { data: entryNamesData } = useQuery({
    queryKey: ["datasetEntryNames", datasetData?.dataset_type, datasetIdNumber],
    queryFn: () =>
      makeRequest<string[]>(
        "GET",
        `api/v1/datasets/${datasetData?.dataset_type}/${datasetId}/entry_names`,
      ),
    enabled:
      datasetIdNumber !== null &&
      !Number.isNaN(datasetIdNumber) &&
      !!datasetData?.dataset_type,
  });

  const { data: recordCountData } = useQuery({
    queryKey: [
      "datasetRecordCount",
      datasetData?.dataset_type,
      datasetIdNumber,
    ],
    queryFn: () =>
      makeRequest<number>(
        "GET",
        `api/v1/datasets/${datasetData?.dataset_type}/${datasetId}/record_count`,
      ),
    enabled:
      datasetIdNumber !== null &&
      !Number.isNaN(datasetIdNumber) &&
      !!datasetData?.dataset_type,
  });

  const { status: attachmentsStatus, data: attachmentsData } = useQuery({
    queryKey: ["datasetAttachments", datasetIdNumber],
    queryFn: () =>
      makeRequest<qcpTypes.DatasetAttachment[]>(
        "GET",
        `api/v1/datasets/${datasetIdNumber}/attachments`,
      ),
    enabled: datasetIdNumber !== null && viewState.tabValue === 3,
  });

  if (!datasetId || datasetIdNumber === null || Number.isNaN(datasetIdNumber)) {
    return <ErrorIndicator fullPage message="Invalid dataset ID" />;
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
                objectId={datasetIdNumber}
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
                  <EditDatasetNameButton dataset={datasetData} />
                </Typography>
                <Typography
                  variant="subtitle1"
                  sx={{ color: "text.secondary" }}
                >
                  {datasetData.tagline}
                </Typography>
              </Box>
              <Stack spacing={1} alignItems="flex-end" sx={{ ml: "auto" }}>
                <Tooltip title="Refresh dataset information">
                  <IconButton onClick={handleRefresh} color="primary">
                    <RefreshIcon />
                  </IconButton>
                </Tooltip>
                <DatasetRelationshipButton datasetId={datasetIdNumber!} />
              </Stack>
            </Box>
          </Grid>
          {/* Summary stats: for example, Specifications, Entries */}
          <Grid size={2}>
            <Paper elevation={3}>
              <Box p={1}>
                <Typography variant="body1" fontWeight="bold">
                  {entryNamesData ? entryNamesData.length : "?"} Entries
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
                    : "?"}{" "}
                  Specifications
                </Typography>
              </Box>
            </Paper>
          </Grid>
          <Grid size={2}>
            <Paper elevation={3}>
              <Box p={1}>
                <Typography variant="body1" fontWeight="bold">
                  {recordCountData !== undefined ? recordCountData : "?"}{" "}
                  Records
                </Typography>
              </Box>
            </Paper>
          </Grid>

          {/* Description & Metadata section */}
          <Grid size={12}>
            <Paper elevation={2}>
              <Box p={2}>
                <Box
                  sx={{
                    display: "flex",
                    alignItems: "center",
                    justifyContent: "space-between",
                  }}
                >
                  <Typography variant="h6" fontWeight="bold" gutterBottom>
                    Description
                  </Typography>
                  <EditDatasetMetadataButton dataset={datasetData} />
                </Box>
                <Typography variant="body1" component={"div"}>
                  <MarkdownContent>
                    {datasetData.description.trim()}
                  </MarkdownContent>
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
          <Grid size={12}>
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
                    page={viewState.statusView.page}
                    specFilter={viewState.statusView.specFilter}
                    onPageChange={(page) => updateStatusView({ page })}
                    onSpecFilterChange={handleStatusSpecFilterChange}
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
                <Tab
                  label="Attachments"
                  id="tab-3"
                  aria-controls="tabpanel-3"
                />
              </Tabs>

              {/* Tab 0: Specifications */}
              <TabPanel value={viewState.tabValue} index={0}>
                {!specificationsData ? (
                  <LoadingIndicator />
                ) : (
                  <DatasetSpecificationTable
                    specificationsData={specificationsData}
                    datasetType={datasetData.dataset_type}
                    datasetId={datasetIdNumber}
                    datasetStatus={statusData}
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
                    datasetId={datasetIdNumber}
                  />
                )}
              </TabPanel>

              {/* Tab 2: Records */}
              <TabPanel value={viewState.tabValue} index={2}>
                {!entryNamesData || !specificationsData || !statusData ? (
                  <LoadingIndicator />
                ) : (
                  <DatasetRecords
                    datasetId={datasetIdNumber}
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

              {/* Tab 3: Attachments */}
              <TabPanel value={viewState.tabValue} index={3}>
                {attachmentsStatus === "pending" && <LoadingIndicator />}
                {attachmentsStatus === "success" && attachmentsData && (
                  <AttachmentTable
                    attachments={attachmentsData}
                    parentType="dataset"
                    parentId={datasetIdNumber}
                    parentName={datasetData.name}
                  />
                )}
                {attachmentsStatus === "error" && (
                  <ErrorIndicator message="Failed to load attachments" />
                )}
              </TabPanel>
            </Paper>
          </Grid>
        </Grid>
      )}
    </>
  );
}
