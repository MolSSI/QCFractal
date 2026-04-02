import React, { useState } from "react";
import * as qcpTypes from "../PortalTypes";
import { Box, Button, Dialog, DialogContent, Typography } from "@mui/material";
import { useQuery } from "@tanstack/react-query";
import { usePortalClient } from "../PortalClient.tsx";
import { GenericDataList } from "./GenericDataList.tsx";
import LoadingIndicator from "./LoadingIndicator";
import ErrorIndicator from "./ErrorIndicator";

interface TaskServiceDetailsProps {
  recordData: qcpTypes.RecordData;
}

export const TaskServiceDetails: React.FC<TaskServiceDetailsProps> = ({
  recordData,
}) => {
  const { makeRequest } = usePortalClient();
  const [taskServiceDialogOpen, setTaskServiceDialogOpen] = useState(false);

  const endpoint = recordData.is_service ? "service" : "task";
  const type = recordData.is_service ? "service" : "task";

  const {
    status: taskStatus,
    data: taskData,
    error: taskError,
  } = useQuery({
    queryKey: ["recordTask", recordData?.record_type, recordData?.id],
    queryFn: () =>
      makeRequest<qcpTypes.RecordTask>(
        "GET",
        `/api/v1/records/${recordData?.record_type}/${recordData?.id}/${endpoint}`,
      ),
    enabled: !!recordData && taskServiceDialogOpen,
  });

  const buttonText = recordData.is_service ? "View Service" : "View Task";

  return (
    <>
      {/* Advance Section */}
      <Button
        variant="outlined"
        size="small"
        disabled={!recordData || recordData?.status == "complete"}
        onClick={() => setTaskServiceDialogOpen(true)}
      >
        {buttonText}
      </Button>

      {/* Modal for Task/Service Fragment */}
      <Dialog
        fullWidth
        open={taskServiceDialogOpen}
        onClose={() => setTaskServiceDialogOpen(false)}
      >
        <DialogContent>
          <Box>
            <Typography variant="h6" fontWeight="bold" sx={{ mb: 1 }}>
              {type.charAt(0).toUpperCase() + type.slice(1)} Details
            </Typography>

            {taskStatus === "pending" && <LoadingIndicator />}

            {taskStatus === "error" && (
              <ErrorIndicator message={taskError.message} />
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
    </>
  );
};

export default TaskServiceDetails;
