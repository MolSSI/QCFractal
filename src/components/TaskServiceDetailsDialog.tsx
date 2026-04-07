import React from "react";
import * as qcpTypes from "../PortalTypes";
import { Box, Dialog, DialogContent, Typography } from "@mui/material";
import { useQuery } from "@tanstack/react-query";
import { usePortalClient } from "../PortalClient.tsx";
import { GenericDataList } from "./GenericDataList.tsx";
import LoadingIndicator from "./LoadingIndicator";
import ErrorIndicator from "./ErrorIndicator";

interface TaskServiceDetailsDialogProps {
  recordId: number;
  recordType: string;
  isService: boolean;
  open: boolean;
  onClose: () => void;
}

export const TaskServiceDetailsDialog: React.FC<TaskServiceDetailsDialogProps> = ({
  recordId,
  recordType,
  isService,
  open,
  onClose,
}) => {
  const { makeRequest } = usePortalClient();

  const type = isService ? "service" : "task";

  const {
    status: taskStatus,
    data: taskData,
    error: taskError,
  } = useQuery({
    queryKey: ["recordTask", recordType, recordId],
    queryFn: () =>
      makeRequest<qcpTypes.RecordTask>(
        "GET",
        `/api/v1/records/${recordType}/${recordId}/${type}`,
      ),
    enabled: open,
  });

  return (
    <Dialog
      fullWidth
      open={open}
      onClose={onClose}
      onClick={(e) => {
        e.stopPropagation();
      }}
    >
      <DialogContent>
        <Box>
          <Typography variant="h6" fontWeight="bold" sx={{ mb: 1 }}>
            {type.charAt(0).toUpperCase() + type.slice(1)} Details
          </Typography>

          {taskStatus === "pending" && <LoadingIndicator />}

          {taskStatus === "error" && (
            <ErrorIndicator message={(taskError as any).message} />
          )}

          {taskStatus === "success" && taskData && (
            <GenericDataList
              data={taskData as Record<string, any>}
              keys={Object.keys(taskData).filter(
                (key) => key !== "function_kwargs_compressed",
              )}
            />
          )}
          {taskStatus === "success" && !taskData && (
            <Typography>No {type} data available for this record.</Typography>
          )}
        </Box>
      </DialogContent>
    </Dialog>
  );
};
