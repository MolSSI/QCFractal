import React, { useState } from "react";
import {
  Box,
  Dialog,
  DialogContent,
  Grid,
  Tab,
  Tabs,
  Typography,
} from "@mui/material";
import { useQuery } from "@tanstack/react-query";
import { usePortalClient } from "../PortalClient.tsx";
import * as qcpTypes from "../PortalTypes";
import LoadingIndicator from "./LoadingIndicator";
import ErrorIndicator from "./ErrorIndicator";
import { stripAnsi } from "../Utils.ts";

interface ViewOutputDialogProps {
  recordType: string;
  recordId: number;
  computeHistoryId: number | undefined;
  initialKey?: string;
  open: boolean;
  onClose: () => void;
}

export const ViewOutputDialog: React.FC<ViewOutputDialogProps> = ({
  recordType,
  recordId,
  computeHistoryId,
  initialKey,
  open,
  onClose,
}) => {
  const [selectedKey, setSelectedKey] = useState<string | null>(initialKey || null);
  const { makeRequest } = usePortalClient();

  const {
    status: historyStatus,
    data: historyData,
    error: historyError,
    isFetching: historyIsFetching,
  } = useQuery({
    queryKey: ["recordComputeHistory", recordId],
    queryFn: () =>
      makeRequest<qcpTypes.ComputeHistory[]>(
        "GET",
        `/api/v1/records/${recordType}/${recordId}/compute_history`,
      ),
    enabled: open && computeHistoryId === undefined,
  });

  const effectiveComputeHistoryId =
    computeHistoryId ??
    (historyData && historyData.length > 0
      ? historyData[historyData.length - 1].id
      : undefined);

  const {
    status: outputKeysStatus,
    data: outputKeysData,
    error: outputKeysError,
  } = useQuery({
    queryKey: [
      "recordOutputs",
      recordType,
      recordId,
      effectiveComputeHistoryId,
    ],
    queryFn: () =>
      makeRequest<Record<string, any>>(
        "GET",
        `/api/v1/records/${recordType}/${recordId}/compute_history/${effectiveComputeHistoryId}/outputs`,
      ),
    enabled: open && effectiveComputeHistoryId !== undefined,
  });

  // Extract keys from fetched data
  const outputKeys = outputKeysData ? Object.keys(outputKeysData) : [];

  // Derive effective key: user selection or first available key
  const effectiveKey =
    selectedKey !== null && outputKeys.includes(selectedKey)
      ? selectedKey
      : outputKeys.length > 0
        ? outputKeys[0]
        : null;

  const {
    status: outputContentStatus,
    data: outputContentData,
    error: outputContentError,
  } = useQuery({
    queryKey: [
      "recordOutputContent",
      recordType,
      recordId,
      effectiveComputeHistoryId,
      effectiveKey,
    ],
    queryFn: () =>
      makeRequest<string>(
        "GET",
        `/api/v1/records/${recordType}/${recordId}/compute_history/${effectiveComputeHistoryId}/outputs/${effectiveKey}/uncompressed_data`,
      ),
    enabled: open && effectiveComputeHistoryId !== undefined && !!effectiveKey,
  });

  return (
    <Dialog
      fullWidth={true}
      maxWidth="lg"
      open={open}
      onClose={onClose}
      onClick={(e) => {
        e.stopPropagation();
      }}
    >
      <DialogContent>
        <>
          {( historyIsFetching || outputKeysStatus === "pending") && (
            <LoadingIndicator />
          )}

          { historyStatus === "error" && (
            <ErrorIndicator message={(historyError as any).message} />
          )}

          {outputKeysStatus === "error" && (
            <ErrorIndicator message={(outputKeysError as any).message} />
          )}

          {historyStatus === "success" &&
            historyData &&
            historyData.length === 0 && (
              <Box sx={{ p: 2 }}>
                <Typography>No compute history found for this record.</Typography>
              </Box>
            )}

          {outputKeysStatus === "success" &&
            outputKeysData &&
            outputKeys.length === 0 && (
              <Box sx={{ p: 2 }}>
                <Typography>No outputs found for this compute history.</Typography>
              </Box>
            )}

          {outputKeysStatus === "success" &&
            outputKeysData &&
            outputKeys.length > 0 && (
              <Grid container spacing={2} width="100%">
                {/* Tabs for output keys */}
                <Grid size={{ xs: 3 }}>
                <Tabs
                  orientation="vertical"
                  value={effectiveKey || false}
                  onChange={(event, newValue) => {
                    event.stopPropagation();
                    setSelectedKey(newValue);
                  }}
                  sx={{ borderRight: 1, borderColor: "divider" }}
                >
                  {outputKeys.map((key) => (
                    <Tab key={key} label={key.toUpperCase()} value={key} />
                  ))}
                </Tabs>
              </Grid>

              {/* Content for the selected key */}
              <Grid size={{ xs: 9 }}>
                {outputContentStatus === "pending" && <LoadingIndicator />}
                {outputContentStatus === "error" && (
                  <ErrorIndicator message={(outputContentError as any).message} />
                )}
                {outputContentStatus === "success" && outputContentData && (
                  <Box
                    sx={{
                      whiteSpace: "pre-wrap",
                      fontFamily: "monospace",
                      overflowY: "auto",
                      maxHeight: "1200px",
                    }}
                  >
                    {/* ... content ... */}
                    {typeof outputContentData === "string" ? (
                      stripAnsi(outputContentData)
                    ) : (
                      // Render object content if the data is not a string
                      <Box component="div">
                        {Object.entries(outputContentData as object).map(
                          ([key, value]) => (
                            <Typography
                              key={key}
                              variant="body2"
                              sx={{ marginBottom: "8px" }}
                            >
                              <strong>{key}:</strong>{" "}
                              {typeof value === "string"
                                ? value
                                : JSON.stringify(value, null, 2)}
                            </Typography>
                          ),
                        )}
                      </Box>
                    )}
                  </Box>
                )}
              </Grid>
            </Grid>
          )}
        </>
      </DialogContent>
    </Dialog>
  );
};
