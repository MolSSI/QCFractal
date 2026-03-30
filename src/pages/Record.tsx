import React, { useState } from "react";
import { usePortalClient } from "../PortalClient.tsx";
import { useParams } from "react-router-dom";
import { WaitingReasonFragment } from "../components/WaitingReasonFragment";
import { ManagerFragment } from "../components/ManagerFragment";
import Comments from "../components/Comments";
import ComputeHistory from "../components/ComputeHistory";
import Specification from "../components/Specification";
import Properties from "../components/Properties";
import * as qcpTypes from "../PortalTypes";
import TaskServiceFragment from "../components/TaskServiceFragment";
import LoadingIndicator from "../components/LoadingIndicator";
import ErrorIndicator from "../components/ErrorIndicator";
import {
  Box,
  Chip,
  Dialog,
  DialogContent,
  Divider,
  Grid,
  Paper,
  Stack,
  Tooltip,
  Typography,
} from "@mui/material";
import HelpOutline from "@mui/icons-material/HelpOutline";
import { format } from "date-fns";
import { MoleculeStageProvider, MoleculeViewer } from "../components/Molecule";
import { getRecordReprMolecule } from "../Utils";
import { useQuery } from "@tanstack/react-query";

function Record() {
  const { recordId } = useParams();
  const { makeRequest } = usePortalClient();
  const [waitingReasonOpen, setWaitingReasonOpen] = React.useState(false);
  const [managerDialogOpen, setManagerDialogOpen] = useState(false);

  const [taskServiceDialogOpen, setTaskServiceDialogOpen] =
    React.useState(false);

  const parsedRecordId = recordId ? Number(recordId) : NaN;
  const validRecordId = Number.isFinite(parsedRecordId);

  const {
    status: recordStatus,
    data: recordData,
    error: recordError,
  } = useQuery({
    queryKey: ["record", parsedRecordId],
    queryFn: () =>
      makeRequest<qcpTypes.RecordData>("GET", `/api/v1/records/${parsedRecordId}`),
    enabled: validRecordId,
  });

  const {
    data: serviceData,
  } = useQuery({
    queryKey: [
      "recordService",
      recordData?.record_type,
      parsedRecordId,
    ],
    queryFn: () =>
      makeRequest<qcpTypes.RecordService>(
        "GET",
        `/api/v1/records/${recordData?.record_type}/${parsedRecordId}/service`,
      ),
    enabled: !!recordData?.record_type && !!recordData?.is_service,
  });

  const {
    data: taskData,
  } = useQuery({
    queryKey: ["recordTask", recordData?.record_type, parsedRecordId],
    queryFn: () =>
      makeRequest<qcpTypes.RecordTask>(
        "GET",
        `/api/v1/records/${recordData?.record_type}/${parsedRecordId}/task`,
      ),
    enabled:
      !!recordData?.record_type &&
      recordData?.is_service !== undefined &&
      !recordData.is_service,
  });

  const moleculeId = recordData ? getRecordReprMolecule(recordData) : undefined;

  const {
    status: moleculeStatus,
    data: moleculeData,
  } = useQuery({
    queryKey: ["molecule", moleculeId],
    queryFn: () =>
      makeRequest<qcpTypes.Molecule>("GET", `api/v1/molecules/${moleculeId}`),
    enabled: !!moleculeId,
  });

  if (!validRecordId) {
    return <ErrorIndicator fullPage message="Invalid record ID" />;
  }

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
      {recordStatus === "pending" && <LoadingIndicator fullPage />}
      {recordStatus === "error" && (
        <ErrorIndicator fullPage message={recordError.message} />
      )}
      {recordStatus === "success" && recordData && (
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
                  "MMMM dd, yyyy HH:mm",
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
                          <WaitingReasonFragment recordId={parsedRecordId} />
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
                {recordData.manager_name ? (
                  <Typography
                    variant="body2"
                    color="primary"
                    sx={{ cursor: "pointer", textDecoration: "underline" }}
                    onClick={() => setManagerDialogOpen(true)}
                  >
                    {recordData.manager_name}
                  </Typography>
                ) : (
                  <Typography variant="body2" color="text.secondary">
                    None
                  </Typography>
                )}
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
                disabled={!serviceData}
                onClick={() => setTaskServiceDialogOpen(true)}
                style={{
                  padding: "8px 16px",
                  backgroundColor: serviceData ? "#1976d2" : "#ccc",
                  color: "#fff",
                  border: "none",
                  borderRadius: "4px",
                  cursor: serviceData ? "pointer" : "not-allowed",
                }}
              >
                View Service
              </button>
            ) : (
              <button
                disabled={!taskData}
                onClick={() => setTaskServiceDialogOpen(true)}
                style={{
                  padding: "8px 16px",
                  backgroundColor: taskData ? "#1976d2" : "#ccc",
                  color: "#fff",
                  border: "none",
                  borderRadius: "4px",
                  cursor: taskData ? "pointer" : "not-allowed",
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
                  <TaskServiceFragment data={serviceData} type="service" />
                ) : (
                  <TaskServiceFragment data={taskData} type="task" />
                )}
              </DialogContent>
            </Dialog>
          </Grid>
          {/* Specification, Properties, and Molecule Viewer */}
          <Grid
            container
            spacing={2}
            sx={{ mt: 2, width: "100%" }}
            alignItems="flex-start"
          >
            {/* Specification */}
            <Grid size={{ xs: 12, sm: 6, lg: 4 }}>
              <Box sx={{ p: 2, height: "100%" }}>
                <Typography variant="h6" fontWeight="bold" sx={{ mb: 1 }}>
                  Specification
                </Typography>
                {recordData.specification ? (
                  <Specification data={recordData.specification} />
                ) : (
                  <Typography>None</Typography>
                )}
              </Box>
            </Grid>

            {/* Properties */}
            <Grid size={{ xs: 12, sm: 6, lg: 4 }}>
              <Properties properties={recordData.properties} />
            </Grid>

            {/* Molecule Viewer */}
            <Grid size={{ xs: 12, sm: 6, lg: 4 }}>
              <Box sx={{ p: 1, display: "flex", flexDirection: "column", alignItems: "center" }}>
                <Typography variant="h6" fontWeight="bold" sx={{ mb: 1, alignSelf: "flex-start" }}>
                  Molecule Viewer
                </Typography>
                <Box
                  sx={{
                    width: "100%",
                    height: 280,
                    backgroundColor: "#e0e0e0",
                    borderRadius: 2,
                    overflow: "hidden",
                    display: "flex",
                    alignItems: "center",
                    justifyContent: "center",
                  }}
                >
                  {!moleculeId ? (
                    <Typography sx={{ color: "#856404", fontSize: "1rem", textAlign: "center", px: 2, fontWeight: 500 }}>
                      No molecule available for this record.
                    </Typography>
                  ) : moleculeStatus === "pending" ? (
                    <LoadingIndicator message="Loading molecule..." />
                  ) : moleculeStatus === "error" ? (
                    <ErrorIndicator message="Failed to load molecule." />
                  ) : moleculeData && typeof moleculeData === "object" ? (
                    <MoleculeStageProvider width="100%" height={280}>
                      <MoleculeViewer moleculeData={moleculeData} />
                    </MoleculeStageProvider>
                  ) : (
                    <Typography sx={{ color: "#856404", fontSize: "1rem", textAlign: "center", px: 2, fontWeight: 500 }}>
                      No molecule available for this record.
                    </Typography>
                  )}
                </Box>
              </Box>
            </Grid>
          </Grid>
        </>
      )}
    </>
  );
}

export default Record;
