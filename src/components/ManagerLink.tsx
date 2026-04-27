import React, { useState } from "react";
import { Typography, Dialog, DialogContent, IconButton } from "@mui/material";
import CloseIcon from "@mui/icons-material/Close";
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
        onClick={(e) => {
          e.stopPropagation();
          setManagerDialogOpen(true);
        }}
      >
        {managerName}
      </Typography>

      <Dialog
        fullWidth={true}
        open={managerDialogOpen}
        onClose={() => {
          setManagerDialogOpen(false);
        }}
        onClick={(e) => {
          e.stopPropagation();
        }}
      >
        <DialogContent sx={{ position: "relative", pt: 5 }}>
          <IconButton
            size="small"
            onClick={() => setManagerDialogOpen(false)}
            sx={{
              position: "absolute",
              right: 8,
              top: 8,
              "&&": { bgcolor: "transparent", border: "none" },
              "&&:hover": { bgcolor: "transparent", border: "none" },
              "&&:active": { bgcolor: "transparent" },
            }}
          >
            <CloseIcon fontSize="small" />
          </IconButton>
          <ManagerFragment managerName={managerName} />
        </DialogContent>
      </Dialog>
    </>
  );
};

export default ManagerLink;
