import React, { useState } from "react";
import * as qcpTypes from "../PortalTypes";
import { Box, Button, Dialog, DialogContent, Typography } from "@mui/material";
import { useQuery } from "@tanstack/react-query";
import { usePortalClient } from "../PortalClient.tsx";
import { GenericDataList } from "./GenericDataList.tsx";

interface TaskServiceDetailsProps {
  recordData: qcpTypes.RecordData;
}

export const TaskServiceDetailsButton: React.FC<TaskServiceDetailsProps> = ({
  recordData,
}) => {
  const { makeRequest } = usePortalClient();
  const [taskServiceDialogOpen, setTaskServiceDialogOpen] = useState(false);

  const endpoint = recordData.is_service ? "service" : "task";

  const { data: taskData } = useQuery({
    queryKey: ["recordTask", recordData?.record_type, recordData?.id],
    queryFn: () =>
      makeRequest<qcpTypes.RecordTask>(
        "GET",
        `/api/v1/records/${recordData?.record_type}/${recordData?.id}/${endpoint}`,
      ),
    enabled: !!recordData,
  });

  const buttonText = recordData.is_service ? "View Service" : "View Task";
  const type = recordData.is_service ? "service" : "task";

  const keys = Object.keys(taskData || {}).filter(
    (key) => key !== "function_kwargs_compressed",
  );

  return (
    <>
      {/* Advance Section */}
      <Button
        disabled={!taskData}
        onClick={() => setTaskServiceDialogOpen(true)}
        style={{
          padding: "8px 16px",
          backgroundColor: taskData ? "#1976d2" : "#ccc",
          color: "#fff",
          border: "none",
          borderRadius: "4px",
          cursor: taskData ? "pointer" : "not-allowed",
        }}
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
            <GenericDataList
              data={taskData as Record<string, any>}
              keys={keys}
            />
          </Box>
        </DialogContent>
      </Dialog>
    </>
  );
};

export default TaskServiceDetailsButton;
