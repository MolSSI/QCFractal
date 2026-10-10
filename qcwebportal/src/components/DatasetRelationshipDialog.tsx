import React, { useState } from "react";
import {
  Box,
  Button,
  CircularProgress,
  Dialog,
  DialogContent,
  DialogTitle,
  IconButton,
  Link as MuiLink,
  List,
  ListItem,
  Stack,
  Typography,
} from "@mui/material";
import CloseIcon from "@mui/icons-material/Close";
import AccountTreeIcon from "@mui/icons-material/AccountTree";
import { useQuery } from "@tanstack/react-query";
import { usePortalClient } from "../PortalClient.tsx";

interface DatasetRelationshipDialogProps {
  datasetId: number;
  open: boolean;
  onClose: () => void;
}

const DatasetRelationshipDialog: React.FC<DatasetRelationshipDialogProps> = ({
  datasetId,
  open,
  onClose,
}) => {
  const { makeRequest } = usePortalClient();

  // Query for projects
  const {
    data: projects,
    isLoading: projectsLoading,
    error: projectsError,
  } = useQuery({
    queryKey: ["dataset-projects", datasetId],
    queryFn: () =>
      makeRequest<any[]>("POST", "/api/v1/projects/querydatasets", {
        dataset_id: [datasetId],
      }),
    enabled: open,
  });

  return (
    <Dialog open={open} onClose={onClose} maxWidth="md" fullWidth>
      <DialogTitle>
        <Box display="flex" justifyContent="space-between" alignItems="center">
          <Typography variant="h6">Relationships</Typography>
          <IconButton onClick={onClose} size="small">
            <CloseIcon />
          </IconButton>
        </Box>
      </DialogTitle>
      <DialogContent dividers>
        <Stack spacing={3}>
          {/* Projects Section */}
          <Box>
            <Typography variant="subtitle1" fontWeight="bold" gutterBottom>
              Projects
            </Typography>
            {projectsLoading ? (
              <CircularProgress size={24} />
            ) : projectsError ? (
              <Typography color="error">Error loading projects</Typography>
            ) : projects && projects.length > 0 ? (
              <List dense>
                {projects.map((proj, index) => (
                  <ListItem key={index} disableGutters>
                    <Stack
                      direction="row"
                      spacing={1}
                      alignItems="flex-start"
                      width="100%"
                    >
                      <Box sx={{ flexGrow: 1 }}>
                        <Typography
                          variant="subtitle1"
                          fontWeight="bold"
                          component="div"
                        >
                          <MuiLink
                            to={`/projects/${proj.project_id}`}
                            target="_blank"
                          >
                            [{proj.project_id}] {proj.project_name}
                          </MuiLink>
                        </Typography>
                        {proj.name && (
                          <List dense sx={{ py: 0 }}>
                            <ListItem disableGutters sx={{ py: 0 }}>
                              <Typography variant="body2">
                                <strong>Dataset Name:</strong> {proj.name}
                              </Typography>
                            </ListItem>
                          </List>
                        )}
                      </Box>
                    </Stack>
                  </ListItem>
                ))}
              </List>
            ) : projects && projects.length === 0 ? (
              <Typography variant="body2" color="text.secondary">
                This dataset is not part of any projects.
              </Typography>
            ) : null}
          </Box>
        </Stack>
      </DialogContent>
    </Dialog>
  );
};

interface DatasetRelationshipButtonProps {
  datasetId: number;
}

export const DatasetRelationshipButton: React.FC<
  DatasetRelationshipButtonProps
> = ({ datasetId }) => {
  const [open, setOpen] = useState(false);

  return (
    <>
      <Button
        variant="outlined"
        size="small"
        startIcon={<AccountTreeIcon />}
        onClick={() => setOpen(true)}
      >
        Relationships
      </Button>
      <DatasetRelationshipDialog
        datasetId={datasetId}
        open={open}
        onClose={() => setOpen(false)}
      />
    </>
  );
};
