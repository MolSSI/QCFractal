import React, { useState } from "react";
import { Button } from "@mui/material";
import { ViewOutputDialog } from "./ViewOutputDialog.tsx";

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

  return (
    <>
      <Button
        variant="outlined"
        size="small"
        onClick={(e) => {
          e.stopPropagation();
          setOpen(true);
        }}
      >
        View Output
      </Button>

      <ViewOutputDialog
        recordType={recordType}
        recordId={recordId}
        computeHistoryId={computeHistoryId}
        open={open}
        onClose={() => {
          setOpen(false);
        }}
      />
    </>
  );
};

export default ViewOutput;
