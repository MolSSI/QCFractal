import React, { useEffect, useState } from "react";
import { useParams } from "react-router-dom";
import { usePortalClientRequest } from "../usePortalClient";
import { WaitingReasonFragment } from "./WaitingReasonFragment";
import { ManagerFragment } from "./ManagerFragment";
import Comments from "./Comments";
import ComputeHistory from "./ComputeHistory";
import Specification from "./Specification";
import Properties from "./Properties";
import * as qcpTypes from "../PortalTypes";
import { FetchedData } from "../PortalClientContext";
import TaskServiceFragment from "./TaskServiceFragment";
import {
  Typography,
  Chip,
  Grid,
  Stack,
  Paper,
  Box,
  Divider,
  Tooltip,
  Dialog,
  DialogContent,
} from "@mui/material";
import HelpOutline from "@mui/icons-material/HelpOutline";
import { format } from "date-fns";
import {MoleculeStageProvider, MoleculeViewer} from "./Molecule";
import { getRecordReprMolecule } from "../Utils";

function Record() {
  const { recordId } = useParams();
  const { fetchData } = usePortalClientRequest();
  const [waitingReasonOpen, setWaitingReasonOpen] = React.useState(false);
  const [managerDialogOpen, setManagerDialogOpen] = useState(false);

  const [recordFetchedData, setRecordFetchedData] = React.useState<
    FetchedData<qcpTypes.RecordData>
  >({
    data: undefined,
    error: undefined,
    loading: true,
  });

  const [serviceFetchedData, setServiceFetchedData] = React.useState<
    FetchedData<qcpTypes.Service>
  >({
    data: undefined,
    error: undefined,
    loading: true,
  });
  const [taskFetchedData, setTaskFetchedData] = React.useState<
    FetchedData<qcpTypes.Task>
  >({
    data: undefined,
    error: undefined,
    loading: true,
  });

  const [moleculeFetchedData, setMoleculeFetchedData] = useState<
    FetchedData<qcpTypes.Molecule>
  >({
    data: undefined,
    error: undefined,
    loading: false,
  });

  const [taskServiceDialogOpen, setTaskServiceDialogOpen] = React.useState(false);

  useEffect(() => {
    setRecordFetchedData({ data: undefined, error: undefined, loading: true });
    setServiceFetchedData({ data: undefined, error: undefined, loading: true });
    setTaskFetchedData({ data: undefined, error: undefined, loading: true });

    fetchData<qcpTypes.RecordData>(
      setRecordFetchedData,
      "get",
      `/api/v1/records/${recordId}`
    );
  }, [fetchData, recordId]);

  useEffect(() => {
    // Only fetch when record data is loaded and record_type is available
    if (
      !recordFetchedData.loading &&
      recordFetchedData.data?.record_type &&
      recordFetchedData.data?.is_service !== undefined
    ) {
      const type = recordFetchedData.data.record_type;
      if (recordFetchedData.data.is_service) {
        fetchData<qcpTypes.Service>(
          setServiceFetchedData,
          "get",
          `/api/v1/records/${type}/${recordId}/service`
        );
      } else {
        fetchData<qcpTypes.Task>(
          setTaskFetchedData,
          "get",
          `/api/v1/records/${type}/${recordId}/task`
        );
      }
    }
  }, [
    fetchData,
    recordFetchedData.loading,
    recordFetchedData.data?.record_type,
    recordFetchedData.data?.is_service,
    recordId,
  ]);

  // Fetch molecule info when recordData is available and moleculeId can be determined
  useEffect(() => {
    if (!recordFetchedData.loading && recordFetchedData?.data) {
      const moleculeId = getRecordReprMolecule(recordFetchedData?.data);
      if (moleculeId) {
        setMoleculeFetchedData({
          data: undefined,
          error: undefined,
          loading: true,
        });
        fetchData<qcpTypes.Molecule>(
          setMoleculeFetchedData,
          "get",
          `api/v1/molecules/${moleculeId}`
        );
      }
    }
  }, [fetchData, recordFetchedData.loading, recordFetchedData?.data]);
  const recordData = recordFetchedData?.data;

  // Status color mapping
  const statusColors: Record<
    string,
    | "success"
    | "error"
    | "warning"
    | "default"
    | "info"
    | "primary"
    | "secondary"
  > = {
    complete: "success",
    error: "error",
    waiting: "warning",
    invalid: "default",
    running: "info",
    cancelled: "primary",
    deleted: "secondary",
  };

  return (
    <>
      {recordFetchedData.loading && <Typography>Loading...</Typography>}
      {!recordFetchedData.loading && recordData && (
        <>
          <Grid
            container
            spacing={2}
            width="100%"
            justifyContent="space-between"
            alignItems="flex-start"
          >
            {/* Main Content */}
            <Grid>
              {/* Name */}
              <Typography variant="h5" fontWeight="bold">
                {recordData.name?.trim() || `Record ${recordId}`} (Record ID{" "}
                {recordId})
              </Typography>

              {/* Description */}
              {recordData.description && (
                <Typography
                  variant="body2"
                  color="text.secondary"
                  sx={{ mt: 1 }}
                >
                  {recordData.description}
                </Typography>
              )}

              {/* Tags */}
              {recordData.tags && (
                <Stack
                  direction="row"
                  spacing={1}
                  sx={{ mt: 1, flexWrap: "wrap" }}
                >
                  {recordData.tags.map((tag, index) => (
                    <Chip
                      key={index}
                      label={tag}
                      variant="outlined"
                      size="small"
                    />
                  ))}
                </Stack>
              )}
            </Grid>

            {/* Additional Info */}
            <Box>
              <Typography variant="body2" sx={{ mb: 1 }}>
                <strong>Created On:</strong>{" "}
                {format(new Date(recordData.created_on), "MMMM dd, yyyy HH:mm")}
              </Typography>
              <Typography variant="body2" sx={{ mb: 1 }}>
                <strong>Modified On:</strong>{" "}
                {format(
                  new Date(recordData.modified_on),
                  "MMMM dd, yyyy HH:mm"
                )}
              </Typography>
              <Typography variant="body2">
                <strong>Creator:</strong> {recordData.owner_group || "None"}
              </Typography>
            </Box>

            {/* Right side Status and Record Type */}
            <Grid
              size={{ xs: 12, sm: "auto" }}
              container
              direction="column"
              alignItems="flex-end"
              justifyContent="flex-start"
            >
              <Stack spacing={1} alignItems="flex-end">
                <Box display="flex" alignItems="center" gap={1}>
                  <Chip
                    label={recordData.status}
                    color={
                      statusColors[recordData.status.toLowerCase()] || "default"
                    }
                    sx={{ fontWeight: "bold", textTransform: "capitalize" }}
                  />
                  {recordData.status.toLowerCase() === "waiting" && (
                    <>
                      <Tooltip title="Click here for waiting reason">
                        <HelpOutline
                          fontSize="small"
                          color="action"
                          sx={{ cursor: "pointer" }}
                          onClick={() => setWaitingReasonOpen(true)}
                        />
                      </Tooltip>
                      <Dialog
                        fullWidth={true}
                        open={waitingReasonOpen}
                        onClose={() => setWaitingReasonOpen(false)}
                      >
                        <DialogContent>
                          <WaitingReasonFragment recordId={Number(recordId)} />
                        </DialogContent>
                      </Dialog>
                    </>
                  )}
                </Box>
                <Paper elevation={3}>
                  <Box p={1}>
                    <Typography variant="body1" fontWeight="bold">
                      Record Type: {recordData.record_type}
                    </Typography>
                  </Box>
                </Paper>
              </Stack>
            </Grid>
          </Grid>
          <Divider sx={{ my: 2, width: "100%" }} />

          <Grid
            container
            spacing={2}
            sx={{ mt: 2, alignItems: "stretch", width: "100%" }}
          >
            {/* Comments Section */}
            <Grid size={{ xs: 12, md: 6 }} sx={{ width: "100%" }}>
              <Box p={0} sx={{ height: "100%" }}>
                <Comments />
              </Box>
            </Grid>

            {/* Last Manager Section */}
            <Grid size={{ xs: 12, md: 6 }} sx={{ width: "100%" }}>
              <Box p={0} sx={{ height: "100%" }}>
                <Typography variant="body1" fontWeight="bold">
                  Last Manager:
                </Typography>
                <Typography
                  variant="body2"
                  color="text.secondary"
                  sx={{ cursor: "pointer", textDecoration: "underline" }}
                  onClick={() => setManagerDialogOpen(true)}
                >
                  {recordData.manager_name || "None"}
                </Typography>
              </Box>

              {/* Manager Dialog */}
              <Dialog
                fullWidth={true}
                open={managerDialogOpen}
                onClose={() => setManagerDialogOpen(false)}
              >
                <DialogContent>
                  <ManagerFragment managerName={recordData.manager_name} />
                </DialogContent>
              </Dialog>
            </Grid>
          </Grid>

          {/* Compute History Section */}
          <Grid size={{ xs: 12 }} sx={{ mt: 2, width: "100%" }}>
            <ComputeHistory
              recordType={recordData.record_type}
              recordId={Number(recordId)}
            />
          </Grid>

          <Divider sx={{ my: 2, width: "100%" }} />

          {/* Advance Section */}
          {/* Title and Button */}
          <Grid container spacing={2} sx={{ mt: 2 }}>
            {/* Title and Button on the same line, left aligned */}

            <Typography variant="h6" fontWeight="bold">
              Advance
            </Typography>
            {recordData.is_service ? (
              <button
                disabled={!serviceFetchedData.data}
                onClick={() => setTaskServiceDialogOpen(true)}
                style={{
                  padding: "8px 16px",
                  backgroundColor: serviceFetchedData.data ? "#1976d2" : "#ccc",
                  color: "#fff",
                  border: "none",
                  borderRadius: "4px",
                  cursor: serviceFetchedData.data ? "pointer" : "not-allowed",
                }}
              >
                View Service
              </button>
            ) : (
              <button
                disabled={!taskFetchedData.data}
                onClick={() => setTaskServiceDialogOpen(true)}
                style={{
                  padding: "8px 16px",
                  backgroundColor: taskFetchedData.data ? "#1976d2" : "#ccc",
                  color: "#fff",
                  border: "none",
                  borderRadius: "4px",
                  cursor: taskFetchedData.data ? "pointer" : "not-allowed",
                }}
              >
                View Task
              </button>
            )}
            {/* Modal for Task/Service Fragment */}
            <Dialog
              fullWidth
              open={taskServiceDialogOpen}
              onClose={() => setTaskServiceDialogOpen(false)}
            >
              <DialogContent>
                {recordData.is_service ? (
                  <TaskServiceFragment
                    data={serviceFetchedData.data}
                    type="service"
                  />
                ) : (
                  <TaskServiceFragment
                    data={taskFetchedData.data}
                    type="task"
                  />
                )}
              </DialogContent>
            </Dialog>
          </Grid>
          {/* Specification and molecule viewer*/}
          <Grid
            container
            spacing={2}
            sx={{ mt: 2, width: "100%" }}
            alignItems="flex-start"
          >
            {/* Specification */}
            <Grid size={{ xs: 12, md: 4 }}>
              <Box
                sx={{
                  p: 2,
                  height: "100%",
                }}
              >
                <Typography variant="h6" fontWeight="bold" sx={{ mb: 1 }}>
                  Specification
                </Typography>
                {recordFetchedData.data?.specification ? (
                  <Specification data={recordFetchedData.data.specification} />
                ) : (
                  <Typography>None</Typography>
                )}
              </Box>
            </Grid>

            {/* Molecule Viewer */}
            <Grid  size={{xs:12, md:8}}>
              <Box sx={{ p: 2, height: "100%" }}>
                <Typography variant="h6" fontWeight="bold" sx={{ mb: 1 }}>
                  Molecule Viewer
                </Typography>
                <Box
                  sx={{
                    width: "100%",
                    height: 300,
                    backgroundColor: "#e0e0e0",
                    borderRadius: 2,
                    position: "relative",
                  }}
                >
                  {moleculeFetchedData.loading ? (
                    <Typography sx={{ p: 2 }}>Loading molecule...</Typography>
                  ) : moleculeFetchedData.data &&
                    typeof moleculeFetchedData.data === "object" ? (
                    <MoleculeStageProvider width={400} height={280}>
                      <MoleculeViewer moleculeData={moleculeFetchedData.data} />
                    </MoleculeStageProvider>
                  ) : (
                    <Typography
                      sx={{
                        color: "#856404",
                        fontSize: "1.15rem",
                        textAlign: "center",
                        px: 2,
                        width: "100%",
                        fontWeight: 500,
                      }}
                    >
                      No molecule available for this record.
                    </Typography>
                  )}
                </Box>
              </Box>
            </Grid>
          </Grid>

          {/* Properties and (right column placeholder) */}
          <Grid container spacing={2} sx={{ mt: 2, width: "100%" }}>
            {/* Properties Column */}
            <Grid size={{ xs: 12, md: 6 }}>
              <Properties properties={recordData.properties} />
            </Grid>

            {/* Right column placeholder */}
            <Grid size={{ xs: 12, md: 6 }}>
              {/* You can add more content here if needed */}
            </Grid>
          </Grid>
        </>
      )}
    </>
  );
}

export default Record;
