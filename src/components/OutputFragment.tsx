import React, { useEffect, useState } from "react";
import { usePortalClientRequest } from "../usePortalClient";
import { FetchedData } from "../PortalClientContext";
import {
  Tabs,
  Tab,
  Typography,
  Box,
  CircularProgress,
  Grid,
} from "@mui/material";

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
  const { fetchData } = usePortalClientRequest();

  // State for fetched output keys
  const [outputKeysFetchedData, setOutputKeysFetchedData] = useState<
    FetchedData<Record<string, any>>
  >({
    data: undefined,
    error: undefined,
    loading: true,
  });

  // State for fetched output content
  const [outputContentFetchedData, setOutputContentFetchedData] = useState<
    FetchedData<string>
  >({
    data: undefined,
    error: undefined,
    loading: true,
  });

  const [selectedKey, setSelectedKey] = useState<string | null>(null);

  // Fetch the keys (e.g., "stdout", "error") from the API
  useEffect(() => {
    fetchData<Record<string, any>>(
      setOutputKeysFetchedData,
      "get",
      `/api/v1/records/${recordType}/${recordId}/compute_history/${computeHistoryId}/outputs`
    );
  }, [recordType, recordId, computeHistoryId, fetchData]);

  // Fetch the content for the selected key
  useEffect(() => {
    if (selectedKey) {
      fetchData<string>(
        setOutputContentFetchedData,
        "get",
        `/api/v1/records/${recordType}/${recordId}/compute_history/${computeHistoryId}/outputs/${selectedKey}/uncompressed_data`
      );
    }
  }, [selectedKey, recordType, recordId, computeHistoryId, fetchData]);

  // Extract keys from fetched data
  const outputKeys = React.useMemo(() => {
    return outputKeysFetchedData.data
      ? Object.keys(outputKeysFetchedData.data)
      : [];
  }, [outputKeysFetchedData.data]);

  // Automatically select the first key when keys are fetched
  useEffect(() => {
    if (outputKeys.length > 0 && selectedKey === null) {
      setSelectedKey(outputKeys[0]);
    }
  }, [outputKeys, selectedKey]);

  return (
    <>
      {outputKeysFetchedData.loading && <Typography>Loading...</Typography>}

      {outputKeysFetchedData.error && (
        <Typography color="error">
          Error: {outputKeysFetchedData.error}
        </Typography>
      )}

      {!outputKeysFetchedData.loading && outputKeysFetchedData.data && (
        <Grid container spacing={2} width="100%">
          {/* Tabs for output keys */}
          <Grid size={{ xs: 3 }}>
            <Tabs
              orientation="vertical"
              value={selectedKey}
              onChange={(event, newValue) => setSelectedKey(newValue)}
              sx={{ borderRight: 1, borderColor: "divider" }}
            >
              {outputKeys.map((key) => (
                <Tab key={key} label={key.toUpperCase()} value={key} />
              ))}
            </Tabs>
          </Grid>

          {/* Content for the selected key */}
          <Grid size={{ xs: 9 }}>
            {outputContentFetchedData.loading && <CircularProgress />}
            {outputContentFetchedData.error && (
              <Typography color="error">
                Error: {outputContentFetchedData.error}
              </Typography>
            )}
            {!outputContentFetchedData.loading &&
              outputContentFetchedData.data && (
                <Box
                  sx={{
                    whiteSpace: "pre-wrap",
                    fontFamily: "monospace",
                    overflowY: "auto",
                    maxHeight: "400px",
                  }}
                >
                  {outputContentFetchedData.data}
                </Box>
              )}
          </Grid>
        </Grid>
      )}
    </>
  );
};

export default OutputFragment;
