import React, { useMemo, useState } from "react";
import {
  Alert,
  Box,
  Button,
  Chip,
  CircularProgress,
  Dialog,
  DialogActions,
  DialogContent,
  DialogContentText,
  DialogTitle,
  IconButton,
  Paper,
  Stack,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  TextField,
  Tooltip,
  Typography,
} from "@mui/material";
import DeleteOutlineIcon from "@mui/icons-material/DeleteOutline";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { usePortalClient } from "../PortalClient";
import { useAuth } from "../Auth";
import * as qcpTypes from "../PortalTypes";

/**
 * Admin panel for managing groups as entities: list existing groups, add a new
 * group, and delete a group. There is no rename endpoint on the backend, so
 * renaming is intentionally not offered. Deleting a group is allowed even when
 * it still has members — those users simply lose the assignment.
 *
 * Member counts are derived client-side from the already-fetched user list.
 */
const GroupsPanel: React.FC<{ users: qcpTypes.UserInfo[] }> = ({ users }) => {
  const { makeRequest } = usePortalClient();
  const { has_permission } = useAuth();
  const queryClient = useQueryClient();

  const canAdd = has_permission("groups", "add");
  const canDelete = has_permission("groups", "delete");

  const [newName, setNewName] = useState("");
  const [newDescription, setNewDescription] = useState("");
  const [addError, setAddError] = useState<string | undefined>();
  const [pendingDelete, setPendingDelete] = useState<qcpTypes.GroupInfo | null>(null);
  const [deleteError, setDeleteError] = useState<string | undefined>();

  const { data: groups, status, error } = useQuery({
    queryKey: ["listGroups"],
    queryFn: () => makeRequest<qcpTypes.GroupInfo[]>("GET", "api/v1/groups"),
    enabled: has_permission("groups", "read"),
  });

  // groupname -> number of users assigned, derived from the user list.
  const memberCounts = useMemo(() => {
    const counts = new Map<string, number>();
    for (const u of users) {
      for (const g of u.groups) {
        counts.set(g, (counts.get(g) ?? 0) + 1);
      }
    }
    return counts;
  }, [users]);

  const existingNames = useMemo(
    () => new Set((groups ?? []).map((g) => g.groupname.toLowerCase())),
    [groups],
  );

  const trimmedName = newName.trim();
  const duplicate = trimmedName !== "" && existingNames.has(trimmedName.toLowerCase());

  const addMutation = useMutation({
    mutationFn: (body: { groupname: string; description?: string }) =>
      makeRequest("POST", "api/v1/groups", body),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["listGroups"] });
      setNewName("");
      setNewDescription("");
      setAddError(undefined);
    },
    onError: (err) => {
      setAddError(err instanceof Error ? err.message : "Failed to add group");
    },
  });

  const deleteMutation = useMutation({
    mutationFn: (groupname: string) =>
      makeRequest("DELETE", `api/v1/groups/${encodeURIComponent(groupname)}`),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["listGroups"] });
      // Group membership lives on users; refresh those too so counts stay honest.
      queryClient.invalidateQueries({ queryKey: ["listUsers"] });
      setPendingDelete(null);
      setDeleteError(undefined);
    },
    onError: (err) => {
      setDeleteError(err instanceof Error ? err.message : "Failed to delete group");
    },
  });

  if (!has_permission("groups", "read")) return null;

  const handleAdd = () => {
    if (!trimmedName || duplicate || addMutation.isPending) return;
    setAddError(undefined);
    addMutation.mutate({
      groupname: trimmedName,
      description: newDescription.trim() || undefined,
    });
  };

  return (
    <Paper variant="outlined" sx={{ p: 2.5, mb: 3 }}>
      <Typography variant="h6" gutterBottom>
        Groups
      </Typography>

      {canAdd && (
        <Box sx={{ mb: 2 }}>
          <Stack direction={{ xs: "column", sm: "row" }} spacing={1.5} alignItems="flex-start">
            <TextField
              label="New group name"
              size="small"
              value={newName}
              onChange={(e) => {
                setNewName(e.target.value);
                setAddError(undefined);
              }}
              onKeyDown={(e) => {
                if (e.key === "Enter") handleAdd();
              }}
              error={duplicate}
              helperText={duplicate ? "A group with this name already exists" : " "}
              sx={{ minWidth: 220 }}
            />
            <TextField
              label="Description (optional)"
              size="small"
              value={newDescription}
              onChange={(e) => setNewDescription(e.target.value)}
              onKeyDown={(e) => {
                if (e.key === "Enter") handleAdd();
              }}
              helperText=" "
              sx={{ minWidth: 260, flexGrow: 1 }}
            />
            <Button
              variant="contained"
              onClick={handleAdd}
              disabled={!trimmedName || duplicate || addMutation.isPending}
              sx={{ mt: 0.5 }}
            >
              {addMutation.isPending ? <CircularProgress size={22} color="inherit" /> : "Add group"}
            </Button>
          </Stack>
          {addError && (
            <Alert severity="error" sx={{ mt: 1 }} onClose={() => setAddError(undefined)}>
              {addError}
            </Alert>
          )}
        </Box>
      )}

      {status === "pending" ? (
        <Box sx={{ display: "flex", justifyContent: "center", py: 3 }}>
          <CircularProgress size={28} />
        </Box>
      ) : status === "error" ? (
        <Alert severity="error">{error.message}</Alert>
      ) : groups && groups.length > 0 ? (
        <TableContainer component={Paper} variant="outlined">
          <Table size="small">
            <TableHead>
              <TableRow>
                <TableCell>Group Name</TableCell>
                <TableCell>Description</TableCell>
                <TableCell align="right">Members</TableCell>
                {canDelete && <TableCell align="right" sx={{ width: 64 }} />}
              </TableRow>
            </TableHead>
            <TableBody>
              {groups.map((g) => {
                const count = memberCounts.get(g.groupname) ?? 0;
                return (
                  <TableRow key={g.groupname} hover>
                    <TableCell>
                      <Typography variant="body2">{g.groupname}</Typography>
                    </TableCell>
                    <TableCell>
                      <Typography variant="body2" color="text.secondary">
                        {g.description || "—"}
                      </Typography>
                    </TableCell>
                    <TableCell align="right">
                      <Chip label={count} size="small" variant="outlined" />
                    </TableCell>
                    {canDelete && (
                      <TableCell align="right">
                        <Tooltip title="Delete group">
                          <IconButton
                            size="small"
                            onClick={() => {
                              setPendingDelete(g);
                              setDeleteError(undefined);
                            }}
                          >
                            <DeleteOutlineIcon fontSize="small" />
                          </IconButton>
                        </Tooltip>
                      </TableCell>
                    )}
                  </TableRow>
                );
              })}
            </TableBody>
          </Table>
        </TableContainer>
      ) : (
        <Typography variant="body2" color="text.secondary">
          (no groups defined)
        </Typography>
      )}

      <Dialog open={pendingDelete !== null} onClose={() => setPendingDelete(null)}>
        <DialogTitle>Delete group?</DialogTitle>
        <DialogContent>
          <DialogContentText>
            {pendingDelete && (
              <>
                Delete the group <strong>{pendingDelete.groupname}</strong>?
                {(() => {
                  const count = memberCounts.get(pendingDelete.groupname) ?? 0;
                  return count > 0 ? (
                    <>
                      {" "}
                      {count} {count === 1 ? "user is" : "users are"} currently assigned to it and
                      will lose this group. This cannot be undone.
                    </>
                  ) : (
                    <> This cannot be undone.</>
                  );
                })()}
              </>
            )}
          </DialogContentText>
          {deleteError && (
            <Alert severity="error" sx={{ mt: 2 }}>
              {deleteError}
            </Alert>
          )}
        </DialogContent>
        <DialogActions>
          <Button onClick={() => setPendingDelete(null)}>Cancel</Button>
          <Button
            color="error"
            variant="contained"
            onClick={() => pendingDelete && deleteMutation.mutate(pendingDelete.groupname)}
            disabled={deleteMutation.isPending}
          >
            {deleteMutation.isPending ? <CircularProgress size={22} color="inherit" /> : "Delete"}
          </Button>
        </DialogActions>
      </Dialog>
    </Paper>
  );
};

export { GroupsPanel };
