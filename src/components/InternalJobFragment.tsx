import { usePortalClient } from "../PortalClient.tsx";
import React from "react";
import * as qcpTypes from "../PortalTypes";
import { parseToDate } from "../Utils";
import { useQuery } from "@tanstack/react-query";
import LoadingIndicator from "./LoadingIndicator";
import ErrorIndicator from "./ErrorIndicator";
import {
  Accordion,
  AccordionDetails,
  AccordionSummary,
  Box,
  Grid,
  IconButton,
  LinearProgress,
  Stack,
  Tooltip,
  Typography,
} from "@mui/material";
import { InternalJobStatusChip } from "./InternalJobStatusChip";
import RefreshIcon from "@mui/icons-material/Refresh";
import ExpandMoreIcon from "@mui/icons-material/ExpandMore";

interface InternalJobFragmentProps {
  internalJobId: number;
  internalJobEndpoint?: string;
}

export const InternalJobFragment: React.FC<InternalJobFragmentProps> = ({
  internalJobId,
  internalJobEndpoint,
}) => {
  const { makeRequest } = usePortalClient();

  const fullEndpoint = internalJobEndpoint
    ? `/api/v1/${internalJobEndpoint}/${internalJobId}`
    : `/api/v1/internal_jobs/${internalJobId}`;

  const {
    status,
    data: internalJobData,
    error,
    refetch,
    isFetching,
  } = useQuery({
    queryKey: ["internalJobInfo", internalJobId],
    queryFn: () => makeRequest<qcpTypes.InternalJob>("GET", fullEndpoint),
  });

  if (status == "pending") {
    return <LoadingIndicator />;
  }

  if (status == "error") {
    return <ErrorIndicator message={error.message} />;
  }

  const addedDate = parseToDate(internalJobData.added_date);
  const scheduledDate = parseToDate(internalJobData.scheduled_date);
  const startedDate = internalJobData.started_date
    ? parseToDate(internalJobData.started_date)
    : null;
  const lastUpdated = internalJobData.last_updated
    ? parseToDate(internalJobData.last_updated)
    : null;
  const endedDate = internalJobData.ended_date
    ? parseToDate(internalJobData.ended_date)
    : null;

  console.log(internalJobData);

  return (
    <Box sx={{ width: "100%" }}>
      <Stack
        direction="row"
        alignItems="center"
        justifyContent="space-between"
        sx={{ mb: 2 }}
      >
        <Stack direction="row" alignItems="center" spacing={1}>
          <Typography variant="h6">
            Internal Job Information {internalJobId}
          </Typography>
          <InternalJobStatusChip status={internalJobData.status} />
        </Stack>
        <Tooltip title="Refresh">
          <IconButton
            onClick={() => refetch()}
            disabled={isFetching}
            size="small"
          >
            <RefreshIcon />
          </IconButton>
        </Tooltip>
      </Stack>

      <Grid container spacing={2}>
        <Grid size={{ xs: 12 }}>
          <Box sx={{ width: "100%", mb: 1 }}>
            <Stack
              direction="row"
              justifyContent="space-between"
              alignItems="center"
            >
              <Typography variant="body2" color="text.secondary">
                Progress: {internalJobData.progress.toFixed(0)}%
              </Typography>
              {internalJobData.progress_description && (
                <Typography variant="body2" color="text.secondary">
                  {internalJobData.progress_description}
                </Typography>
              )}
            </Stack>
            <LinearProgress
              variant="determinate"
              value={internalJobData.progress}
              sx={{ height: 10, borderRadius: 5 }}
            />
          </Box>
        </Grid>

        <Grid size={{ xs: 12, md: 6 }}>
          <Typography variant="subtitle2" color="text.secondary">
            General Information
          </Typography>
          <Typography variant="body2">
            <strong>Name:</strong> {internalJobData.name}
          </Typography>
          <Typography variant="body2">
            <strong>Function:</strong> {internalJobData.function}
          </Typography>
          <Typography variant="body2">
            <strong>User:</strong> {internalJobData.user || "(none)"}
          </Typography>
          <Typography variant="body2">
            <strong>Serial Group:</strong>{" "}
            {internalJobData.serial_group || "(none)"}
          </Typography>
          <Typography variant="body2">
            <strong>Repeat Delay:</strong>{" "}
            {internalJobData.repeat_delay ?? "(none)"}
          </Typography>
        </Grid>

        <Grid size={{ xs: 12, md: 6 }}>
          <Typography variant="subtitle2" color="text.secondary">
            Dates
          </Typography>
          <Typography variant="body2">
            <strong>Added:</strong> {addedDate?.toLocaleString() || "(unknown)"}
          </Typography>
          <Typography variant="body2">
            <strong>Scheduled:</strong>{" "}
            {scheduledDate?.toLocaleString() || "(unknown)"}
          </Typography>
          <Typography variant="body2">
            <strong>Started:</strong>{" "}
            {startedDate?.toLocaleString() || "(not started)"}
          </Typography>
          <Typography variant="body2">
            <strong>Last Updated:</strong>{" "}
            {lastUpdated?.toLocaleString() || "(not updated)"}
          </Typography>
          <Typography variant="body2">
            <strong>Ended:</strong>{" "}
            {endedDate?.toLocaleString() || "(not ended)"}
          </Typography>
        </Grid>

        <Grid size={{ xs: 12, md: 6 }}>
          <Typography variant="subtitle2" color="text.secondary">
            Runner Information
          </Typography>
          <Typography variant="body2">
            <strong>Hostname:</strong>{" "}
            {internalJobData.runner_hostname || "(none)"}
          </Typography>
          <Typography variant="body2">
            <strong>UUID:</strong> {internalJobData.runner_uuid || "(none)"}
          </Typography>
        </Grid>

        <Grid size={{ xs: 12 }}>
          <Accordion variant="outlined" sx={{ mt: 1 }}>
            <AccordionSummary expandIcon={<ExpandMoreIcon />}>
              <Typography variant="subtitle2">Advanced Information</Typography>
            </AccordionSummary>
            <AccordionDetails>
              <Stack spacing={2}>
                <Typography variant="subtitle2" color="text.secondary">
                  Result
                </Typography>
                {!internalJobData.result && "(none)"}
                {internalJobData.result && (
                  <Grid size={{ xs: 12 }}>
                    <Box
                      component="pre"
                      sx={{
                        p: 1,
                        backgroundColor: "action.hover",
                        borderRadius: 1,
                        overflow: "auto",
                        fontSize: "0.8rem",
                      }}
                    >
                      {typeof internalJobData.result === "string"
                        ? internalJobData.result
                        : JSON.stringify(internalJobData.result, null, 2)}
                    </Box>
                  </Grid>
                )}
                <Typography variant="subtitle2" color="text.secondary">
                  Arguments (kwargs)
                </Typography>
                {(Object.keys(internalJobData.kwargs).length === 0 ||
                  !internalJobData.kwargs) &&
                  "(none)"}
                {internalJobData.kwargs &&
                  Object.keys(internalJobData.kwargs).length > 0 && (
                    <Box>
                      <Box
                        component="pre"
                        sx={{
                          p: 1,
                          backgroundColor: "action.hover",
                          borderRadius: 1,
                          overflow: "auto",
                          fontSize: "0.8rem",
                          mt: 0.5,
                        }}
                      >
                        {JSON.stringify(internalJobData.kwargs, null, 2)}
                      </Box>
                    </Box>
                  )}

                <Typography variant="subtitle2" color="text.secondary">
                  After Function Arguments (after_function_kwargs)
                </Typography>
                {!internalJobData.after_function_kwargs && "(none)"}
                {internalJobData.after_function_kwargs &&
                  Object.keys(internalJobData.after_function_kwargs).length >
                    0 && (
                    <Box>
                      <Box
                        component="pre"
                        sx={{
                          p: 1,
                          backgroundColor: "action.hover",
                          borderRadius: 1,
                          overflow: "auto",
                          fontSize: "0.8rem",
                          mt: 0.5,
                        }}
                      >
                        {JSON.stringify(
                          internalJobData.after_function_kwargs,
                          null,
                          2,
                        )}
                      </Box>
                    </Box>
                  )}
              </Stack>
            </AccordionDetails>
          </Accordion>
        </Grid>
      </Grid>
    </Box>
  );
};
