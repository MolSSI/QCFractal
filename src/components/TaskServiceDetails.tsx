import React, { useState } from "react";
import { Button } from "@mui/material";
import { TaskServiceDetailsDialog } from "./TaskServiceDetailsDialog.tsx";

interface TaskServiceDetailsProps {
  recordId: number;
  recordType: string;
  isService: boolean;
  disabled: boolean;
}

export const TaskServiceDetails: React.FC<TaskServiceDetailsProps> = ({
  recordId,
  recordType,
  isService,
  disabled,
}) => {
  const [taskServiceDialogOpen, setTaskServiceDialogOpen] = useState(false);

  const buttonText = isService ? "View Service" : "View Task";

  return (
    <>
      <Button
        variant="outlined"
        size="small"
        disabled={disabled}
        onClick={(e) => {
          e.stopPropagation();
          setTaskServiceDialogOpen(true);
        }}
      >
        {buttonText}
      </Button>

      <TaskServiceDetailsDialog
        recordId={recordId}
        recordType={recordType}
        isService={isService}
        open={taskServiceDialogOpen}
        onClose={() => setTaskServiceDialogOpen(false)}
      />
    </>
  );
};

export default TaskServiceDetails;
