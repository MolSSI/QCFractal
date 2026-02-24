import {
  FetchedData,
  usePortalClient,
} from "../PortalClient.tsx";
import React, { useEffect, useState } from "react";
import * as qcpTypes from "../PortalTypes";
import { IconButton, Stack, Typography } from "@mui/material";
import KeyboardArrowDownIcon from "@mui/icons-material/KeyboardArrowDown";
import KeyboardArrowUpIcon from "@mui/icons-material/KeyboardArrowUp";

export const WaitingReasonFragment: React.FC<{ recordId: number }> = ({
  recordId,
}) => {
  const { fetchData } = usePortalClient(); // Get client instance here

  const [reasonFetchedData, setReasonFetchedData] = useState<
    FetchedData<qcpTypes.WaitingReason>
  >({
    data: undefined,
    error: undefined,
    loading: true,
  });

  useEffect(() => {
    fetchData<qcpTypes.WaitingReason>(
      setReasonFetchedData,
      "get",
      `api/v1/records/${recordId}/waiting_reason`,
    );
  }, [fetchData, recordId]);

  const waitingReason = reasonFetchedData?.data;

  const [detailsExpanded, setDetailsExpanded] = useState(false);

  return (
    <>
      {reasonFetchedData.loading && <Typography>Loading...</Typography>}

      {!reasonFetchedData.loading && waitingReason && (
        <Stack>
          <Typography variant="h5">Reason</Typography>
          <Typography variant="body1">
            {waitingReason.reason ? waitingReason.reason : "(unknown)"}
          </Typography>

          {waitingReason.details && (
            <Stack spacing={4} paddingTop={4}>
              <Stack
                direction="row"
                justifyContent="space-between"
                onClick={() => setDetailsExpanded(!detailsExpanded)}
                sx={{ cursor: "pointer" }}
              >
                <Typography variant="h5" paddingBottom={3}>
                  Details
                </Typography>
                <IconButton size="small">
                  {detailsExpanded ? (
                    <KeyboardArrowUpIcon />
                  ) : (
                    <KeyboardArrowDownIcon />
                  )}
                </IconButton>
              </Stack>
              {detailsExpanded && (
                <Stack spacing={1} height={400} sx={{ overflowY: "auto" }}>
                  {Object.entries(waitingReason.details).map(([manager, r]) => {
                    return (
                      <>
                        <Typography component="dt" variant="body1">
                          <strong>{manager}</strong>
                        </Typography>
                        <Typography
                          component="dd"
                          paddingLeft="20px"
                          variant="body1"
                        >
                          {r}
                        </Typography>
                      </>
                    );
                  })}
                </Stack>
              )}
            </Stack>
          )}
        </Stack>
      )}
    </>
  );
};
