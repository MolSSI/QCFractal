import React, { useEffect, useState } from "react";
import { usePortalClient } from "../PortalClient.tsx";
import {
  Box,
  Grid,
  Tab,
  Tabs,
  Typography,
} from "@mui/material";
import { useQuery } from "@tanstack/react-query";
import LoadingIndicator from "./LoadingIndicator";
import ErrorIndicator from "./ErrorIndicator";

interface OutputFragmentProps {
  recordType: string;
  recordId: number;
  computeHistoryId: number;
}

const OutputFragment: React.FC<OutputFragmentProps> = ({
  recordType,
  recordId,
  computeHistoryId,
}) => {
  const { makeRequest } = usePortalClient();

  const [selectedKey, setSelectedKey] = useState<string | null>(null);

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
  });

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
      selectedKey,
    ],
    queryFn: () =>
      makeRequest<string>(
        "GET",
        `/api/v1/records/${recordType}/${recordId}/compute_history/${computeHistoryId}/outputs/${selectedKey}/uncompressed_data`,
      ),
    enabled: !!selectedKey,
  });

  // Extract keys from fetched data
  const outputKeys = React.useMemo(() => {
    return outputKeysData ? Object.keys(outputKeysData) : [];
  }, [outputKeysData]);

  // Automatically select the first key when keys are fetched
  useEffect(() => {
    if (outputKeys.length > 0 && selectedKey === null) {
      setSelectedKey(outputKeys[0]);
    }
  }, [outputKeys, selectedKey]);

  return (
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
              value={selectedKey || outputKeys[0] || false}
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
                    maxHeight: "400px",
                  }}
                >
                  {typeof outputContentData === "string" ? (
                    outputContentData
                  ) : (
                    // Render object content if the data is not a string
                    <Box component="div">
                      {Object.entries(outputContentData).map(
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
  );
};

export default OutputFragment;
