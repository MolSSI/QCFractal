import React from "react";
import { usePortalClient } from "../PortalClient.tsx";
import * as qcpTypes from "../PortalTypes";
import { useParams } from "react-router-dom";
import {
  Box,
  Grid,
  Paper,
  Typography,
  Chip,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  TablePagination,
  Collapse,
  IconButton,
  Tabs,
  Tab,
} from "@mui/material";
import KeyboardArrowDownIcon from "@mui/icons-material/KeyboardArrowDown";
import KeyboardArrowUpIcon from "@mui/icons-material/KeyboardArrowUp";
import LoadingIndicator from "../components/LoadingIndicator";
import ErrorIndicator from "../components/ErrorIndicator";
import { useQuery } from "@tanstack/react-query";

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

function EntryRow({
  entryName,
  datasetType,
  datasetId,
}: {
  entryName: string;
  datasetType: string;
  datasetId: string;
}) {
  const [expanded, setExpanded] = React.useState(false);
  const { makeRequest } = usePortalClient();

  const { data: entryData, status } = useQuery({
    queryKey: ["datasetEntry", datasetType, datasetId, entryName],
    queryFn: () =>
      makeRequest<Record<string, unknown>>(
        "POST",
        `api/v1/datasets/${datasetType}/${datasetId}/entries/bulkFetch`,
        { names: [entryName] },
      ),
    enabled: expanded,
  });

  return (
    <React.Fragment>
      <TableRow
        hover
        sx={{ cursor: "pointer" }}
        onClick={() => setExpanded(!expanded)}
      >
        <TableCell width="50px">
          <IconButton size="small">
            {expanded ? <KeyboardArrowUpIcon /> : <KeyboardArrowDownIcon />}
          </IconButton>
        </TableCell>
        <TableCell>
          <Typography variant="body2" fontWeight="medium">
            {entryName}
          </Typography>
        </TableCell>
      </TableRow>
      <TableRow>
        <TableCell style={{ paddingBottom: 0, paddingTop: 0 }} colSpan={2}>
          <Collapse in={expanded} timeout="auto" unmountOnExit>
            <Box
              sx={{
                margin: 1,
                p: 2,
                bgcolor: (theme) =>
                  theme.palette.mode === "dark" ? "grey.900" : "grey.100",
                color: "text.primary",
                borderRadius: 1,
              }}
            >
              {status === "pending" && <LoadingIndicator />}
              {status === "error" && (
                <Typography color="error">Error fetching entry data</Typography>
              )}
              {status === "success" && entryData && (
                <pre style={{ margin: 0, overflow: "auto" }}>
                  {JSON.stringify(entryData[entryName], null, 2)}
                </pre>
              )}
            </Box>
          </Collapse>
        </TableCell>
      </TableRow>
    </React.Fragment>
  );
}

export default function Dataset() {
  const { datasetId } = useParams();
  const { makeRequest } = usePortalClient();

  const [tabValue, setTabValue] = React.useState(0);

  const [pageSpec, setPageSpec] = React.useState(0);
  const [rowsPerPageSpec, setRowsPerPageSpec] = React.useState(10);
  const [expandedSpecName, setExpandedSpecName] = React.useState<string | null>(
    null,
  );

  const [pageEntry, setPageEntry] = React.useState(0);
  const [rowsPerPageEntry, setRowsPerPageEntry] = React.useState(10);

  const handleTabChange = (_event: React.SyntheticEvent, newValue: number) => {
    setTabValue(newValue);
  };

  const {
    status: datasetStatus,
    data: datasetData,
    error: datasetError,
  } = useQuery({
    queryKey: ["dataset", datasetId],
    queryFn: () => makeRequest<qcpTypes.Dataset>("GET", `api/v1/datasets/${datasetId}`),
    enabled: !!datasetId,
  });

  const {
    data: specificationsData,
  } = useQuery({
    queryKey: ["datasetSpecifications", datasetData?.dataset_type, datasetId],
    queryFn: () => makeRequest<Record<string, unknown>>("GET", `api/v1/datasets/${datasetData?.dataset_type}/${datasetId}/specifications`),
    enabled: !!datasetId && !!datasetData?.dataset_type,
  });

  const {
    data: entryNamesData,
  } = useQuery({
    queryKey: ["datasetEntryNames", datasetData?.dataset_type, datasetId],
    queryFn: () => makeRequest<string[]>("GET", `api/v1/datasets/${datasetData?.dataset_type}/${datasetId}/entry_names`),
    enabled: !!datasetId && !!datasetData?.dataset_type,
  });

  const {
    data: recordCountData,
  } = useQuery({
    queryKey: ["datasetRecordCount", datasetData?.dataset_type, datasetId],
    queryFn: () => makeRequest<number>("GET", `api/v1/datasets/${datasetData?.dataset_type}/${datasetId}/record_count`),
    enabled: !!datasetId && !!datasetData?.dataset_type,
  });

  const handleChangePageSpec = (_event: unknown, newPage: number) => {
    setPageSpec(newPage);
  };

  const handleChangeRowsPerPageSpec = (
    event: React.ChangeEvent<HTMLInputElement>,
  ) => {
    setRowsPerPageSpec(parseInt(event.target.value, 10));
    setPageSpec(0);
  };

  const handleChangePageEntry = (_event: unknown, newPage: number) => {
    setPageEntry(newPage);
  };

  const handleChangeRowsPerPageEntry = (
    event: React.ChangeEvent<HTMLInputElement>,
  ) => {
    setRowsPerPageEntry(parseInt(event.target.value, 10));
    setPageEntry(0);
  };

  const toggleExpandSpec = (name: string) => {
    setExpandedSpecName((prev) => (prev === name ? null : name));
  };

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
              <Box p={2} borderBottom={1} borderColor="divider">
                <Typography variant="h6" fontWeight="bold">
                  Description & Metadata
                </Typography>
              </Box>
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

          {/* Tabs for Specifications, Entries, Records */}
          <Grid size={12} mt={2}>
            <Paper elevation={2}>
              <Tabs
                value={tabValue}
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
              <TabPanel value={tabValue} index={0}>
                {!specificationsData ? (
                  <LoadingIndicator />
                ) : (
                  <>
                    <TableContainer component={Paper} variant="outlined">
                      <Table size="small">
                        <TableHead>
                          <TableRow>
                            <TableCell width="50px" />
                            <TableCell>
                              <strong>Name</strong>
                            </TableCell>
                          </TableRow>
                        </TableHead>
                        <TableBody>
                          {Object.keys(specificationsData)
                            .slice(
                              pageSpec * rowsPerPageSpec,
                              pageSpec * rowsPerPageSpec + rowsPerPageSpec,
                            )
                            .map((specName) => (
                              <React.Fragment key={specName}>
                                <TableRow
                                  hover
                                  sx={{ cursor: "pointer" }}
                                  onClick={() => toggleExpandSpec(specName)}
                                >
                                  <TableCell>
                                    <IconButton size="small">
                                      {expandedSpecName === specName ? (
                                        <KeyboardArrowUpIcon />
                                      ) : (
                                        <KeyboardArrowDownIcon />
                                      )}
                                    </IconButton>
                                  </TableCell>
                                  <TableCell>
                                    <Typography
                                      variant="body2"
                                      fontWeight="medium"
                                    >
                                      {specName}
                                    </Typography>
                                  </TableCell>
                                </TableRow>
                                <TableRow>
                                  <TableCell
                                    style={{ paddingBottom: 0, paddingTop: 0 }}
                                    colSpan={2}
                                  >
                                    <Collapse
                                      in={expandedSpecName === specName}
                                      timeout="auto"
                                      unmountOnExit
                                    >
                                      <Box
                                        sx={{
                                          margin: 1,
                                          p: 2,
                                          bgcolor: (theme) =>
                                            theme.palette.mode === "dark"
                                              ? "grey.900"
                                              : "grey.100",
                                          color: "text.primary",
                                          borderRadius: 1,
                                        }}
                                      >
                                        <pre
                                          style={{
                                            margin: 0,
                                            overflow: "auto",
                                          }}
                                        >
                                          {JSON.stringify(
                                            specificationsData[specName],
                                            null,
                                            2,
                                          )}
                                        </pre>
                                      </Box>
                                    </Collapse>
                                  </TableCell>
                                </TableRow>
                              </React.Fragment>
                            ))}
                        </TableBody>
                      </Table>
                    </TableContainer>
                    <TablePagination
                      rowsPerPageOptions={[10, 25, 50]}
                      component="div"
                      count={Object.keys(specificationsData).length}
                      rowsPerPage={rowsPerPageSpec}
                      page={pageSpec}
                      onPageChange={handleChangePageSpec}
                      onRowsPerPageChange={handleChangeRowsPerPageSpec}
                    />
                  </>
                )}
              </TabPanel>

              {/* Tab 1: Entries */}
              <TabPanel value={tabValue} index={1}>
                {!entryNamesData ? (
                  <LoadingIndicator />
                ) : (
                  <>
                    <TableContainer component={Paper} variant="outlined">
                      <Table size="small">
                        <TableHead>
                          <TableRow>
                            <TableCell width="50px" />
                            <TableCell>
                              <strong>Name</strong>
                            </TableCell>
                          </TableRow>
                        </TableHead>
                        <TableBody>
                          {entryNamesData
                            .slice(
                              pageEntry * rowsPerPageEntry,
                              pageEntry * rowsPerPageEntry + rowsPerPageEntry,
                            )
                            .map((entryName) => (
                              <EntryRow
                                key={entryName}
                                entryName={entryName}
                                datasetType={datasetData.dataset_type}
                                datasetId={datasetId}
                              />
                            ))}
                        </TableBody>
                      </Table>
                    </TableContainer>
                    <TablePagination
                      rowsPerPageOptions={[10, 25, 50]}
                      component="div"
                      count={entryNamesData.length}
                      rowsPerPage={rowsPerPageEntry}
                      page={pageEntry}
                      onPageChange={handleChangePageEntry}
                      onRowsPerPageChange={handleChangeRowsPerPageEntry}
                    />
                  </>
                )}
              </TabPanel>

              {/* Tab 2: Records (blank for now) */}
              <TabPanel value={tabValue} index={2}>
                <Typography variant="body1">
                  Records tab is coming soon.
                </Typography>
              </TabPanel>
            </Paper>
          </Grid>
        </Grid>
      )}
    </>
  );
}
