import React, { useState } from "react";
import { Typography, Dialog, DialogContent } from "@mui/material";
import { ManagerFragment } from "./ManagerFragment";

interface ManagerLinkProps {
  managerName: string;
}

export const ManagerLink: React.FC<ManagerLinkProps> = ({ managerName }) => {
  const [managerDialogOpen, setManagerDialogOpen] = useState(false);

  return (
    <>
      <Typography
        variant="body2"
        color="primary"
        sx={{
          cursor: "pointer",
          textDecoration: "underline",
        }}
        onClick={() => setManagerDialogOpen(true)}
      >
        {managerName}
      </Typography>

      <Dialog
        fullWidth={true}
        open={managerDialogOpen}
        onClose={() => setManagerDialogOpen(false)}
      >
        <DialogContent>
          <ManagerFragment managerName={managerName} />
        </DialogContent>
      </Dialog>
    </>
  );
};

export default ManagerLink;
