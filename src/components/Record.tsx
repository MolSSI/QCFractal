import React, { useEffect, useState } from "react";
import { useParams } from "react-router-dom";
import { usePortalClientRequest } from "../usePortalClient";
import { WaitingReasonFragment } from "./WaitingReasonFragment";
import { ManagerFragment } from "./ManagerFragment";
import Comments from "./Comments";
import ComputeHistory from "./ComputeHistory";  
import * as qcpTypes from "../PortalTypes";
import { FetchedData } from "../PortalClientContext";
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
import HelpOutline from '@mui/icons-material/HelpOutline';
import { format } from "date-fns"; 


function Record() {
  const { projectId, recordId } = useParams();
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

  // Fetch record data
  useEffect(() => {
    fetchData<qcpTypes.RecordData>(
      setRecordFetchedData,
      "get",
      `/api/v1/projects/${projectId}/records/${recordId}`
    );
  }, [fetchData, projectId, recordId]);

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

          <Grid container spacing={2} width="80%">
            {/* Comments Section */}
            <Grid size={{ xs: 12, md: 6 }}>
              <Box>
                <Comments />
              </Box>
            </Grid>

            {/* Last Manager Section */}
            <Grid size={{ xs: 12, md: 6 }}>
              <Paper elevation={3}>
                <Box p={2}>
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
              </Paper>
        
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

        </>
      )}
    </>
  );
};

export default Record;
