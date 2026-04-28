import { usePortalClient } from "../PortalClient.tsx";
import { useParams } from "react-router-dom";
import * as qcpTypes from "../PortalTypes";
import LoadingIndicator from "../components/LoadingIndicator";
import ErrorIndicator from "../components/ErrorIndicator";

import { useQuery } from "@tanstack/react-query";

import { getRecordDetailsComponent } from "../components/record_components/lookup";
import React, { useEffect } from "react";
import {
  Box,
  Button,
  Chip,
  Divider,
  Grid,
  Stack,
  Typography,
} from "@mui/material";
import StatusChip from "../components/StatusChip.tsx";
import RecordTypeChip from "../components/RecordTypeChip.tsx";
import { RecordRelationshipButton } from "../components/RecordRelationshipDialog.tsx";
import Comments from "../components/Comments.tsx";
import ComputeHistory from "../components/ComputeHistory.tsx";
import { FavoriteButton } from "../components/FavoriteButton.tsx";
import { ViewOutputButton } from "../components/ViewOutputDialog.tsx";
import { TaskServiceButton } from "../components/TaskServiceDialog.tsx";
import { dateStringToLocalTime } from "../Utils.ts";

interface RecordHeaderProps {
  recordData: qcpTypes.RecordData;
}

export const RecordHeader: React.FC<RecordHeaderProps> = ({ recordData }) => {
  const computeHistory = recordData.compute_history || [];
  const nComputeHistory = computeHistory.length;

  return (
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
          <Stack direction="row" alignItems="flex-start" spacing={1}>
            <FavoriteButton
              preferencesKey="favorite_records"
              objectId={recordData.id}
            />
            {/* Name */}
            <Typography variant="h5" fontWeight="bold">
              {recordData.name
                ? recordData.name.trim()
                : `Record ${recordData.id}`}
            </Typography>
          </Stack>

          {/* Description */}
          {recordData.description && (
            <Typography variant="body2" color="text.secondary" sx={{ mt: 1 }}>
              {recordData.description}
            </Typography>
          )}

          {/* Tags */}
          {recordData.tags && (
            <Stack direction="row" spacing={1} sx={{ mt: 1, flexWrap: "wrap" }}>
              {recordData.tags.map((tag, index) => (
                <Chip key={index} label={tag} variant="outlined" size="small" />
              ))}
            </Stack>
          )}
        </Grid>

        {/* Additional Info */}
        <Box>
          <Typography variant="body2" sx={{ mb: 1 }}>
            <strong>Created On:</strong>{" "}
            {dateStringToLocalTime(recordData.created_on)}
          </Typography>
          <Typography variant="body2" sx={{ mb: 1 }}>
            <strong>Modified On:</strong>{" "}
            {dateStringToLocalTime(recordData.modified_on)}
          </Typography>
          <Typography variant="body2">
            <strong>Creator:</strong> {recordData.owner_group || "(none)"}
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
            <StatusChip
              status={recordData.status}
              recordId={recordData.id}
              recordType={recordData.record_type}
            />
            <RecordTypeChip type={recordData.record_type} />
            <RecordRelationshipButton recordId={recordData.id} />
            {recordData.is_service && (
              <Chip
                label="Service"
                color="secondary"
                sx={{ fontWeight: "bold", textTransform: "capitalize" }}
              />
            )}
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
            <Comments comments={recordData.comments} />
          </Box>
        </Grid>

        {/* Various buttons */}
        <Box display="flex" gap={1}>
          <ViewOutputButton
            recordType={recordData.record_type}
            recordId={recordData.id}
            computeHistoryId={
              nComputeHistory > 0
                ? computeHistory[nComputeHistory - 1].id
                : undefined
            }
          />
          <Button variant="outlined" size="small" disabled>
            Native Files
          </Button>
          <Button variant="outlined" size="small" disabled>
            Reset
          </Button>
          <TaskServiceButton
            recordId={recordData.id}
            recordType={recordData.record_type}
            isService={recordData.is_service}
            disabled={recordData.status === "complete"}
          />
        </Box>

        {/* Compute History Section */}
        {!recordData.is_service && (
          <Grid size={{ xs: 12 }} sx={{ mt: 2, width: "100%" }}>
            <ComputeHistory recordData={recordData} />
          </Grid>
        )}
      </Grid>
      <Divider sx={{ my: 2, width: "100%" }} />
    </>
  );
};

function Record() {
  const { recordId } = useParams();
  const { makeRequest } = usePortalClient();

  const parsedRecordId = recordId ? Number(recordId) : NaN;
  const validRecordId = Number.isFinite(parsedRecordId);

  const {
    status: recordStatus,
    data: recordData,
    error: recordError,
  } = useQuery({
    queryKey: ["record", parsedRecordId],
    queryFn: () =>
      makeRequest<qcpTypes.RecordData>(
        "GET",
        `/api/v1/records/${parsedRecordId}`,
        undefined,
        { include: ["*", "compute_history", "comments"] },
      ),
    enabled: validRecordId,
  });

  useEffect(() => {
    if (recordData) {
      const recordName = recordData.name ? recordData.name.trim() : "";
      document.title = `Record ${recordData.id}${recordName ? ": " + recordName : ""}`;
    } else {
      document.title = `Record ${parsedRecordId}`;
    }
  }, [recordData, parsedRecordId]);

  if (!validRecordId) {
    return <ErrorIndicator fullPage message="Invalid record ID" />;
  }

  if (recordStatus === "pending") {
    return <LoadingIndicator fullPage />;
  }
  if (recordStatus === "error") {
    return <ErrorIndicator fullPage message={recordError.message} />;
  }
  if (!recordData) {
    return <ErrorIndicator fullPage message="Record not found" />;
  }

  //const RecordComponent = getRecordDetailsComponent(recordData.record_type);
  const RecordComponent = getRecordDetailsComponent(recordData.record_type);

  return (
    <>
      <RecordHeader recordData={recordData} />

      {/* eslint-disable-next-line react-hooks/static-components */}
      <RecordComponent recordData={recordData} />
    </>
  );
}

export default Record;
