import React, { useMemo, useState } from "react";
import {
  Alert,
  Box,
  Button,
  Checkbox,
  Chip,
  CircularProgress,
  FormControl,
  Grid,
  InputLabel,
  Link as MuiLink,
  MenuItem,
  Paper,
  Select,
  Snackbar,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TablePagination,
  TableRow,
  TextField,
  Typography,
} from "@mui/material";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { usePortalClient } from "../PortalClient";
import { useAuth } from "../Auth";
import * as qcpTypes from "../PortalTypes";
import LoadingIndicator from "../components/LoadingIndicator";
import ErrorIndicator from "../components/ErrorIndicator";
import { RoleChip } from "../components/RoleChip";
import { GroupsPanel } from "../components/GroupsPanel";
import { AddUserDialog } from "../components/AddUserDialog";
import PersonAddIcon from "@mui/icons-material/PersonAdd";
import { usePageTitle } from "../UsePageTitle.ts";

const UserRow: React.FC<{
  user: qcpTypes.UserInfo;
  selectable: boolean;
  selected: boolean;
  onToggle: (username: string) => void;
}> = ({ user, selectable, selected, onToggle }) => {
  return (
    <TableRow hover selected={selected}>
      {selectable && (
        <TableCell padding="checkbox">
          <Checkbox
            size="small"
            checked={selected}
            onChange={() => onToggle(user.username)}
          />
        </TableCell>
      )}
      <TableCell>
        <MuiLink
          to={`/users/${user.username}`}
          variant="body2"
          sx={{ color: "common.white", textDecoration: "underline" }}
        >
          {user.username}
        </MuiLink>
      </TableCell>
      <TableCell>
        <Typography variant="body2">{user.fullname ?? "—"}</Typography>
      </TableCell>
      <TableCell>
        <Typography variant="body2" color="text.secondary">{user.email ?? "—"}</Typography>
      </TableCell>
      <TableCell>
        <RoleChip role={user.role} />
      </TableCell>
      <TableCell>
        {user.groups.length > 0 ? (
          <Box sx={{ display: "flex", flexWrap: "wrap", gap: 0.5 }}>
            {user.groups.map((g) => (
              <Chip key={g} label={g} size="small" variant="outlined" />
            ))}
          </Box>
        ) : (
          <Typography variant="body2" color="text.secondary">—</Typography>
        )}
      </TableCell>
      <TableCell>
        <Chip
          label={user.enabled ? "Active" : "Disabled"}
          size="small"
          color={user.enabled ? "success" : "default"}
          variant="outlined"
        />
      </TableCell>
      {/* <TableCell>
        <Chip label={user.auth_type} size="small" variant="outlined" />
      </TableCell> */}
    </TableRow>
  );
};

const UserList: React.FC = () => {
  const { makeRequest } = usePortalClient();
  const { has_permission } = useAuth();
  const queryClient = useQueryClient();
  const [filter, setFilter] = useState("");
  const [statusFilter, setStatusFilter] = useState<"all" | "enabled" | "disabled">("all");
  const [page, setPage] = useState(0);
  const [rowsPerPage, setRowsPerPage] = useState(20);
  const [selected, setSelected] = useState<Set<string>>(new Set());
  const [assignGroup, setAssignGroup] = useState("");
  const [addUserOpen, setAddUserOpen] = useState(false);
  const [toast, setToast] = useState<
    { severity: "success" | "error"; message: string } | undefined
  >();

  usePageTitle("User Management");

  const canModifyUsers = has_permission("users", "modify");
  const canAddUsers = has_permission("users", "add");

  const { status, data: users, error } = useQuery({
    queryKey: ["listUsers"],
    queryFn: () => makeRequest<qcpTypes.UserInfo[]>("GET", "api/v1/users"),
    enabled: has_permission("users", "read"),
  });

  // Groups are used to populate the batch-assign dropdown. Reuses the shared
  // ["listGroups"] key so it stays in sync with the GroupsPanel above.
  const { data: groups } = useQuery({
    queryKey: ["listGroups"],
    queryFn: () => makeRequest<qcpTypes.GroupInfo[]>("GET", "api/v1/groups"),
    enabled: (canModifyUsers || canAddUsers) && has_permission("groups", "read"),
  });

  const existingUsernames = useMemo(
    () => new Set((users ?? []).map((u) => u.username.toLowerCase())),
    [users],
  );

  const filtered = useMemo(() => {
    if (!users) return [];
    const q = filter.toLowerCase();
    return users.filter((u) => {
      if (statusFilter === "enabled" && !u.enabled) return false;
      if (statusFilter === "disabled" && u.enabled) return false;
      return (
        u.username.toLowerCase().includes(q) ||
        (u.fullname ?? "").toLowerCase().includes(q) ||
        (u.email ?? "").toLowerCase().includes(q)
      );
    });
  }, [users, filter, statusFilter]);

  const selectable = canModifyUsers && (groups?.length ?? 0) > 0;

  // Header checkbox operates on the full filtered set (across pages).
  const allFilteredSelected =
    filtered.length > 0 && filtered.every((u) => selected.has(u.username));
  const someFilteredSelected = filtered.some((u) => selected.has(u.username));

  const toggleOne = (username: string) => {
    setSelected((prev) => {
      const next = new Set(prev);
      if (next.has(username)) next.delete(username);
      else next.add(username);
      return next;
    });
  };

  const toggleAllFiltered = () => {
    setSelected((prev) => {
      const next = new Set(prev);
      if (allFilteredSelected) filtered.forEach((u) => next.delete(u.username));
      else filtered.forEach((u) => next.add(u.username));
      return next;
    });
  };

  const assignMutation = useMutation({
    mutationFn: async (groupname: string) => {
      const targets = (users ?? []).filter((u) => selected.has(u.username));
      const results = await Promise.allSettled(
        targets.map((u) =>
          u.groups.includes(groupname)
            ? Promise.resolve() // already a member; nothing to do
            : makeRequest<void>("PATCH", "api/v1/users", {
                ...u,
                groups: [...u.groups, groupname],
              }),
        ),
      );
      const failed = results.filter((r) => r.status === "rejected").length;
      return { total: targets.length, failed, groupname };
    },
    onSuccess: ({ total, failed, groupname }) => {
      queryClient.invalidateQueries({ queryKey: ["listUsers"] });
      setSelected(new Set());
      setAssignGroup("");
      if (failed === 0) {
        setToast({
          severity: "success",
          message: `Assigned ${total} ${total === 1 ? "user" : "users"} to "${groupname}".`,
        });
      } else {
        setToast({
          severity: "error",
          message: `Assigned ${total - failed} of ${total} users to "${groupname}"; ${failed} failed.`,
        });
      }
    },
    onError: (err) => {
      setToast({
        severity: "error",
        message: err instanceof Error ? err.message : "Failed to assign group",
      });
    },
  });

  const selectedCount = selected.size;

  if (!has_permission("users", "read")) {
    return (
      <Box sx={{ display: "flex", alignItems: "center", justifyContent: "center", minHeight: "70vh" }}>
        <Typography color="text.secondary">You do not have permission to view users.</Typography>
      </Box>
    );
  }

  if (status === "pending") {
    return (
      <Box sx={{ display: "flex", alignItems: "center", justifyContent: "center", minHeight: "70vh" }}>
        <LoadingIndicator />
      </Box>
    );
  }

  if (status === "error") {
    return (
      <Box sx={{ display: "flex", alignItems: "center", justifyContent: "center", minHeight: "70vh" }}>
        <ErrorIndicator message={error.message} />
      </Box>
    );
  }

  return (
    <Grid container spacing={2} width="100%">
      <Grid size={12}>
        <Box
          sx={{
            display: "flex",
            alignItems: "center",
            justifyContent: "space-between",
            flexWrap: "wrap",
            gap: 2,
            mb: 3,
          }}
        >
          <Typography variant="h4">User Management</Typography>
          {canAddUsers && (
            <Button
              variant="contained"
              startIcon={<PersonAddIcon />}
              onClick={() => setAddUserOpen(true)}
            >
              Add User
            </Button>
          )}
        </Box>
        <GroupsPanel users={users} />
        <Box sx={{ display: "flex", gap: 2, mb: 2, flexWrap: "wrap" }}>
          <TextField
            variant="outlined"
            size="small"
            label="Search users by username"
            value={filter}
            onChange={(e) => { setFilter(e.target.value); setPage(0); }}
            sx={{ width: "30%", minWidth: 220 }}
          />
          <FormControl size="small" sx={{ minWidth: 160 }}>
            <InputLabel id="status-filter-label">Status</InputLabel>
            <Select
              labelId="status-filter-label"
              label="Status"
              value={statusFilter}
              onChange={(e) => {
                setStatusFilter(e.target.value as "all" | "enabled" | "disabled");
                setPage(0);
              }}
            >
              <MenuItem value="all">All</MenuItem>
              <MenuItem value="enabled">Active</MenuItem>
              <MenuItem value="disabled">Disabled</MenuItem>
            </Select>
          </FormControl>
        </Box>
        {selectable && selectedCount === 0 && (
          <Typography variant="body2" color="text.secondary" sx={{ mb: 2 }}>
            Tip: select one or more users to batch-assign them to a group.
          </Typography>
        )}
        {selectable && selectedCount > 0 && (
          <Paper
            variant="outlined"
            sx={{
              mb: 2,
              px: 2,
              py: 1.5,
              display: "flex",
              alignItems: "center",
              gap: 2,
              flexWrap: "wrap",
            }}
          >
            <Typography variant="body2" sx={{ fontWeight: 500 }}>
              {selectedCount} selected
            </Typography>
            <FormControl size="small" sx={{ minWidth: 220 }}>
              <InputLabel id="assign-group-label">Assign to group</InputLabel>
              <Select
                labelId="assign-group-label"
                label="Assign to group"
                value={assignGroup}
                onChange={(e) => setAssignGroup(e.target.value)}
              >
                {(groups ?? []).map((g) => (
                  <MenuItem key={g.groupname} value={g.groupname}>
                    {g.groupname}
                  </MenuItem>
                ))}
              </Select>
            </FormControl>
            <Button
              variant="contained"
              disabled={!assignGroup || assignMutation.isPending}
              onClick={() => assignMutation.mutate(assignGroup)}
            >
              {assignMutation.isPending ? (
                <CircularProgress size={22} color="inherit" />
              ) : (
                "Assign"
              )}
            </Button>
            <Button
              color="inherit"
              onClick={() => setSelected(new Set())}
              disabled={assignMutation.isPending}
            >
              Clear
            </Button>
          </Paper>
        )}
        {filtered.length > 0 ? (
          <>
            <TableContainer component={Paper} variant="outlined">
              <Table size="small">
                <TableHead>
                  <TableRow>
                    {selectable && (
                      <TableCell padding="checkbox">
                        <Checkbox
                          size="small"
                          checked={allFilteredSelected}
                          indeterminate={someFilteredSelected && !allFilteredSelected}
                          onChange={toggleAllFiltered}
                        />
                      </TableCell>
                    )}
                    <TableCell>Username</TableCell>
                    <TableCell>Full Name</TableCell>
                    <TableCell>Email</TableCell>
                    <TableCell>Role</TableCell>
                    <TableCell>Groups</TableCell>
                    <TableCell>Status</TableCell>
                  </TableRow>
                </TableHead>
                <TableBody>
                  {filtered
                    .slice(page * rowsPerPage, page * rowsPerPage + rowsPerPage)
                    .map((user) => (
                      <UserRow
                        key={user.username}
                        user={user}
                        selectable={selectable}
                        selected={selected.has(user.username)}
                        onToggle={toggleOne}
                      />
                    ))}
                </TableBody>
              </Table>
            </TableContainer>
            <TablePagination
              rowsPerPageOptions={[10, 20, 50]}
              component="div"
              count={filtered.length}
              rowsPerPage={rowsPerPage}
              page={page}
              onPageChange={(_e, p) => setPage(p)}
              onRowsPerPageChange={(e) => { setRowsPerPage(parseInt(e.target.value, 10)); setPage(0); }}
            />
          </>
        ) : (
          <Typography variant="body1">
            {filter ? "(no users match the filter)" : "(no users found)"}
          </Typography>
        )}
      </Grid>
      {canAddUsers && (
        <AddUserDialog
          open={addUserOpen}
          onClose={() => setAddUserOpen(false)}
          existingUsernames={existingUsernames}
          groups={groups ?? []}
          onCreated={(username) =>
            setToast({
              severity: "success",
              message: `User "${username}" created.`,
            })
          }
        />
      )}
      <Snackbar
        open={!!toast}
        autoHideDuration={5000}
        onClose={() => setToast(undefined)}
        anchorOrigin={{ vertical: "bottom", horizontal: "center" }}
      >
        {toast ? (
          <Alert
            severity={toast.severity}
            variant="filled"
            onClose={() => setToast(undefined)}
          >
            {toast.message}
          </Alert>
        ) : undefined}
      </Snackbar>
    </Grid>
  );
};

export { UserList };
