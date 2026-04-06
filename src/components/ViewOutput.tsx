import React, { useState } from "react";
import {
  Box,
  Button,
  Dialog,
  DialogContent,
  Grid,
  Tab,
  Tabs,
  Typography,
} from "@mui/material";
import { useQuery } from "@tanstack/react-query";
import { usePortalClient } from "../PortalClient.tsx";
import LoadingIndicator from "./LoadingIndicator";
import ErrorIndicator from "./ErrorIndicator";

interface ViewOutputButtonProps {
  recordType: string;
  recordId: number;
  computeHistoryId: number | undefined;
}

export const ViewOutput: React.FC<ViewOutputButtonProps> = ({
  recordType,
  recordId,
  computeHistoryId,
}) => {
  const [open, setOpen] = useState(false);
  const [selectedKey, setSelectedKey] = useState<string | null>(null);
  const { makeRequest } = usePortalClient();

  const {
    status: outputKeysStatus,
    data: outputKeysData,
    error: outputKeysError,
  } = useQuery({
    queryKey: ["recordOutputs", recordType, recordId, computeHistoryId],
    queryFn: () =>
      makeRequest<Record<string, any>>(
        "GET",
        `/api/v1/records/${recordType}/${recordId}/compute_history/${computeHistoryId}/outputs`,
      ),
    enabled: open && computeHistoryId !== undefined,
  });

  // Extract keys from fetched data
  const outputKeys = outputKeysData ? Object.keys(outputKeysData) : [];

  // Derive effective key: user selection or first available key
  const effectiveKey = selectedKey ?? outputKeys[0] ?? null;

  const {
    status: outputContentStatus,
    data: outputContentData,
    error: outputContentError,
  } = useQuery({
    queryKey: [
      "recordOutputContent",
      recordType,
      recordId,
      computeHistoryId,
      effectiveKey,
    ],
    queryFn: () =>
      makeRequest<string>(
        "GET",
        `/api/v1/records/${recordType}/${recordId}/compute_history/${computeHistoryId}/outputs/${effectiveKey}/uncompressed_data`,
      ),
    enabled: open && computeHistoryId !== undefined && !!effectiveKey,
  });

  return (
    <>
      <Button
        variant="outlined"
        size="small"
        disabled={computeHistoryId === undefined}
        onClick={() => setOpen(true)}
      >
        View Output
      </Button>

      <Dialog
        fullWidth={true}
        maxWidth="lg"
        open={open}
        onClose={() => setOpen(false)}
      >
        <DialogContent>
          <>
            {outputKeysStatus === "pending" && <LoadingIndicator />}

            {outputKeysStatus === "error" && (
              <ErrorIndicator message={outputKeysError.message} />
            )}

            {outputKeysStatus === "success" && outputKeysData && (
              <Grid container spacing={2} width="100%">
                {/* Tabs for output keys */}
                <Grid size={{ xs: 3 }}>
                  <Tabs
                    orientation="vertical"
                    value={effectiveKey || false}
                    onChange={(_event, newValue) => setSelectedKey(newValue)}
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
                    <ErrorIndicator message={outputContentError.message} />
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
                      {typeof outputContentData === "string" ? (
                        outputContentData
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
    </>
  );
};

export default ViewOutput;
