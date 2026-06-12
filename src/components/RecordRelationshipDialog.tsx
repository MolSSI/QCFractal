import React, { useState } from "react";
import {
  Box,
  Button,
  CircularProgress,
  Dialog,
  DialogContent,
  DialogTitle,
  Divider,
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
import * as qcpTypes from "../PortalTypes";
import StatusChip from "./StatusChip.tsx";
import RecordTypeChip from "./RecordTypeChip.tsx";
import { createDatasetRecordsLocationState } from "./dataset_components/DatasetViewState.tsx";

interface RecordRelationshipDialogProps {
  recordId: number;
  open: boolean;
  onClose: () => void;
}

const RecordRelationshipDialog: React.FC<RecordRelationshipDialogProps> = ({
  recordId,
  open,
  onClose,
}) => {
  const { makeRequest } = usePortalClient();

  // Query for datasets
  const {
    data: datasets,
    isLoading: datasetsLoading,
    error: datasetsError,
  } = useQuery({
    queryKey: ["record-datasets", recordId],
    queryFn: () =>
      makeRequest<any[]>("POST", "/api/v1/datasets/queryrecords", {
        record_id: [recordId],
      }),
    enabled: open,
  });

  // Query for projects
  const {
    data: projects,
    isLoading: projectsLoading,
    error: projectsError,
  } = useQuery({
    queryKey: ["record-projects", recordId],
    queryFn: () =>
      makeRequest<any[]>("POST", "/api/v1/projects/queryrecords", {
        record_id: [recordId],
      }),
    enabled: open,
  });
  console.log("Projects query status:", { projects });

  // Query for parent records
  const {
    data: parents,
    isLoading: parentsLoading,
    error: parentsError,
  } = useQuery({
    queryKey: ["record-parents", recordId],
    queryFn: async () => {
      const parentIds = await makeRequest<number[]>(
        "POST",
        "/api/v1/records/query",
        {
          child_id: [recordId],
        },
      );
      if (parentIds.length === 0) return [];
      return makeRequest<qcpTypes.BaseRecord[]>(
        "POST",
        "/api/v1/records/bulkGet",
        {
          ids: parentIds,
        },
        {
          include: ["id", "record_type", "status"],
        },
      );
    },
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
          {/* Datasets Section */}
          <Box>
            <Typography variant="subtitle1" fontWeight="bold" gutterBottom>
              Datasets
            </Typography>
            {datasetsLoading ? (
              <CircularProgress size={24} />
            ) : datasetsError ? (
              <Typography color="error">Error loading datasets</Typography>
            ) : datasets && datasets.length > 0 ? (
              <List dense>
                {datasets.map((ds, index) => (
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
                            to={`/datasets/${ds.dataset_id}`}
                            state={createDatasetRecordsLocationState(
                              String(ds.dataset_id),
                              ds.entry_name,
                              ds.specification_name,
                            )}
                          >
                            [{ds.dataset_id}] {ds.dataset_name}
                          </MuiLink>
                        </Typography>
                        <List dense sx={{ py: 0 }}>
                          <ListItem disableGutters sx={{ py: 0 }}>
                            <Typography variant="body2">
                              <strong>Entry:</strong> {ds.entry_name}
                            </Typography>
                          </ListItem>
                          <ListItem disableGutters sx={{ py: 0 }}>
                            <Typography variant="body2">
                              <strong>Specification:</strong>{" "}
                              {ds.specification_name}
                            </Typography>
                          </ListItem>
                        </List>
                      </Box>
                      <Box sx={{ mt: 0.5 }}>
                        <RecordTypeChip type={ds.dataset_type} />
                      </Box>
                    </Stack>
                  </ListItem>
                ))}
              </List>
            ) : datasets && datasets.length === 0 ? (
              <Typography variant="body2" color="text.secondary">
                This record is not part of any datasets.
              </Typography>
            ) : null}
          </Box>

          <Divider />

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
                        <List dense sx={{ py: 0 }}>
                          <ListItem disableGutters sx={{ py: 0 }}>
                            <Typography variant="body2">
                              <strong>Record Name:</strong> {proj.record_name}
                            </Typography>
                          </ListItem>
                        </List>
                      </Box>
                    </Stack>
                  </ListItem>
                ))}
              </List>
            ) : projects && projects.length === 0 ? (
              <Typography variant="body2" color="text.secondary">
                This record is not part of any projects.
              </Typography>
            ) : null}
          </Box>

          <Divider />

          {/* Parent Records Section */}
          <Box>
            <Typography variant="subtitle1" fontWeight="bold" gutterBottom>
              Parent Records
            </Typography>
            {parentsLoading ? (
              <CircularProgress size={24} />
            ) : parentsError ? (
              <Typography color="error">
                Error loading parent records
              </Typography>
            ) : parents && parents.length > 0 ? (
              <List dense>
                {parents.map((parent) => (
                  <ListItem key={parent.id} disableGutters>
                    <Stack
                      direction="row"
                      spacing={1}
                      alignItems="center"
                      width="100%"
                    >
                      <MuiLink
                        to={`/records/${parent.id}`}
                        target="_blank"
                      >
                        Record {parent.id}
                      </MuiLink>
                      <Box sx={{ flexGrow: 1 }} />
                      <RecordTypeChip type={parent.record_type} />
                      <StatusChip
                        status={parent.status}
                        recordId={parent.id}
                        recordType={parent.record_type}
                      />
                    </Stack>
                  </ListItem>
                ))}
              </List>
            ) : parents && parents.length === 0 ? (
              <Typography variant="body2" color="text.secondary">
                This record has no parent records.
              </Typography>
            ) : null}
          </Box>
        </Stack>
      </DialogContent>
    </Dialog>
  );
};

interface RecordRelationshipButtonProps {
  recordId: number;
}

export const RecordRelationshipButton: React.FC<
  RecordRelationshipButtonProps
> = ({ recordId }) => {
  const [open, setOpen] = useState(false);

  return (
    <>
      <Button
        variant="outlined"
        size="small"
        startIcon={<AccountTreeIcon />}
        onClick={() => setOpen(true)}
        sx={{ mt: 1 }}
      >
        Relationships
      </Button>
      <RecordRelationshipDialog
        recordId={recordId}
        open={open}
        onClose={() => setOpen(false)}
      />
    </>
  );
};
