import React, { useMemo, useState } from "react";
import {
  Box,
  Chip,
  Grid,
  Link as MuiLink,
  Paper,
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
import { useQuery } from "@tanstack/react-query";
import { usePortalClient } from "../PortalClient";
import { useAuth } from "../Auth";
import * as qcpTypes from "../PortalTypes";
import LoadingIndicator from "../components/LoadingIndicator";
import ErrorIndicator from "../components/ErrorIndicator";
import { RoleChip } from "../components/RoleChip";
import { GroupsPanel } from "../components/GroupsPanel";
import { usePageTitle } from "../UsePageTitle.ts";

const UserRow: React.FC<{ user: qcpTypes.UserInfo }> = ({ user }) => {
  return (
    <TableRow hover>
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
  const [filter, setFilter] = useState("");
  const [page, setPage] = useState(0);
  const [rowsPerPage, setRowsPerPage] = useState(20);

  usePageTitle("Users");

  const { status, data: users, error } = useQuery({
    queryKey: ["listUsers"],
    queryFn: () => makeRequest<qcpTypes.UserInfo[]>("GET", "api/v1/users"),
    enabled: has_permission("users", "read"),
  });

  const filtered = useMemo(() => {
    if (!users) return [];
    const q = filter.toLowerCase();
    return users.filter(
      (u) =>
        u.username.toLowerCase().includes(q) ||
        (u.fullname ?? "").toLowerCase().includes(q) ||
        (u.email ?? "").toLowerCase().includes(q),
    );
  }, [users, filter]);

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
        <Typography variant="h4" marginBottom={3}>Users</Typography>
        <GroupsPanel users={users} />
        <Box width="30%" mb={2}>
          <TextField
            fullWidth
            variant="outlined"
            size="small"
            label="Filter users"
            value={filter}
            onChange={(e) => { setFilter(e.target.value); setPage(0); }}
          />
        </Box>
        {filtered.length > 0 ? (
          <>
            <TableContainer component={Paper} variant="outlined">
              <Table size="small">
                <TableHead>
                  <TableRow>
                    <TableCell>Username</TableCell>
                    <TableCell>Full Name</TableCell>
                    <TableCell>Email</TableCell>
                    <TableCell>Role</TableCell>
                    <TableCell>Status</TableCell>
                  </TableRow>
                </TableHead>
                <TableBody>
                  {filtered
                    .slice(page * rowsPerPage, page * rowsPerPage + rowsPerPage)
                    .map((user) => (
                      <UserRow key={user.username} user={user} />
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
    </Grid>
  );
};

export { UserList };
