import React, { useMemo, useState } from "react";
import {
  Alert,
  Box,
  Button,
  Chip,
  Divider,
  IconButton,
  InputAdornment,
  Link as MuiLink,
  Paper,
  Stack,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TablePagination,
  TableRow,
  TextField,
  Tooltip,
  Typography,
} from "@mui/material";
import AddIcon from "@mui/icons-material/Add";
import DeleteIcon from "@mui/icons-material/Delete";
import SearchIcon from "@mui/icons-material/Search";
import { useQuery } from "@tanstack/react-query";
import { Link } from "react-router-dom";
import * as qcpTypes from "../PortalTypes";
import { usePortalClient } from "../PortalClient.tsx";
import { useAuth } from "../Auth.tsx";
import { usePageTitle } from "../UsePageTitle.ts";
import {
  dateStringToLocalTime,
  describeRequestError,
  matchesTokens,
} from "../Utils.ts";
import LoadingIndicator from "../components/LoadingIndicator";
import ErrorIndicator from "../components/ErrorIndicator";
import {
  CreateApiKeyDialog,
  DeleteApiKeyDialog,
  isExpired,
} from "../components/ApiKeyDialogs.tsx";

const ALL_TOKENS_QUERY_KEY = ["allApiTokens"];

const ApiKeyAdmin: React.FC = () => {
  const { makeRequest } = usePortalClient();
  const { has_permission } = useAuth();
  usePageTitle("API Key Management");

  const canModifyUsers = has_permission("users", "modify");

  const [filter, setFilter] = useState("");
  const [page, setPage] = useState(0);
  const [rowsPerPage, setRowsPerPage] = useState(25);
  const [createOpen, setCreateOpen] = useState(false);
  const [tokenToRevoke, setTokenToRevoke] = useState<qcpTypes.APIToken | null>(
    null,
  );

  const {
    status,
    data: tokens,
    error,
  } = useQuery({
    queryKey: ALL_TOKENS_QUERY_KEY,
    queryFn: () => makeRequest<qcpTypes.APIToken[]>("GET", "api/v1/tokens"),
  });

  const { data: users } = useQuery({
    queryKey: ["listUsers"],
    queryFn: () => makeRequest<qcpTypes.UserInfo[]>("GET", "api/v1/users"),
  });

  const usernameById = useMemo(() => {
    const map = new Map<number, string>();
    for (const user of users ?? []) {
      if (user.id !== undefined) {
        map.set(user.id, user.username);
      }
    }
    return map;
  }, [users]);

  const filtered = useMemo(() => {
    const all = tokens ?? [];
    if (!filter.trim()) return all;
    return all.filter(
      (t) =>
        matchesTokens(filter, t.name) ||
        matchesTokens(filter, usernameById.get(t.user_id)) ||
        matchesTokens(filter, t.token_prefix),
    );
  }, [tokens, filter, usernameById]);

  const paged = filtered.slice(
    page * rowsPerPage,
    page * rowsPerPage + rowsPerPage,
  );

  const revokeOwner = tokenToRevoke && usernameById.get(tokenToRevoke.user_id);

  return (
    <Box width="100%" sx={{ p: 2 }}>
      <Stack
        direction="row"
        alignItems="flex-start"
        justifyContent="space-between"
        spacing={2}
        sx={{ mb: 1 }}
      >
        <Box>
          <Typography variant="h4" gutterBottom>
            API Key Management
          </Typography>
          <Typography variant="body1" color="text.secondary">
            Every API key on the server. Keys themselves are never stored in
            readable form and cannot be shown here — only their metadata.
          </Typography>
        </Box>
        {canModifyUsers && (
          <Button
            variant="contained"
            startIcon={<AddIcon />}
            onClick={() => setCreateOpen(true)}
            sx={{ flexShrink: 0 }}
          >
            Create key for user
          </Button>
        )}
      </Stack>

      {!canModifyUsers && (
        <Alert severity="info" sx={{ mt: 2 }}>
          You can review keys but not issue or revoke them. That requires an
          administrator.
        </Alert>
      )}

      <Paper sx={{ p: 3, mt: 3 }}>
        <Stack
          direction="row"
          alignItems="center"
          justifyContent="space-between"
          spacing={2}
        >
          <Typography variant="h6">
            All keys{status === "success" ? ` (${filtered.length})` : ""}
          </Typography>
          <TextField
            value={filter}
            onChange={(e) => {
              setFilter(e.target.value);
              setPage(0);
            }}
            size="small"
            placeholder="Filter by user, key name, or prefix"
            sx={{ width: 320 }}
            slotProps={{
              input: {
                startAdornment: (
                  <InputAdornment position="start">
                    <SearchIcon fontSize="small" />
                  </InputAdornment>
                ),
              },
            }}
          />
        </Stack>
        <Divider sx={{ my: 2 }} />

        {status === "pending" && <LoadingIndicator message="Loading keys..." />}
        {status === "error" && (
          <ErrorIndicator
            message={describeRequestError(error, "Failed to load API keys")}
          />
        )}
        {status === "success" &&
          (filtered.length === 0 ? (
            <Typography variant="body2" color="text.secondary" sx={{ py: 2 }}>
              {tokens.length === 0
                ? "No API keys exist on this server yet."
                : "No keys match this filter."}
            </Typography>
          ) : (
            <>
              <TableContainer>
                <Table size="small">
                  <TableHead>
                    <TableRow>
                      <TableCell>User</TableCell>
                      <TableCell>Name</TableCell>
                      <TableCell>Prefix</TableCell>
                      <TableCell>Created</TableCell>
                      <TableCell>Expires</TableCell>
                      <TableCell>Last used</TableCell>
                      {canModifyUsers && (
                        <TableCell align="right">Actions</TableCell>
                      )}
                    </TableRow>
                  </TableHead>
                  <TableBody>
                    {paged.map((token) => {
                      const username = usernameById.get(token.user_id);
                      return (
                        <TableRow key={token.id} hover>
                          <TableCell>
                            {username ? (
                              <MuiLink
                                component={Link}
                                to={`/users/${username}`}
                                variant="body2"
                              >
                                {username}
                              </MuiLink>
                            ) : (
                              <Typography
                                variant="body2"
                                color="text.secondary"
                              >
                                user #{token.user_id}
                              </Typography>
                            )}
                          </TableCell>
                          <TableCell>
                            <Stack
                              direction="row"
                              spacing={1}
                              alignItems="center"
                              flexWrap="wrap"
                            >
                              <Typography variant="body2">
                                {token.name}
                              </Typography>
                              {isExpired(token) && (
                                <Chip
                                  label="Expired"
                                  size="small"
                                  color="error"
                                  variant="outlined"
                                />
                              )}
                              {token.scope !== "unlimited" && (
                                <Chip
                                  label={token.scope || "no scope"}
                                  size="small"
                                  color="warning"
                                  variant="outlined"
                                />
                              )}
                            </Stack>
                          </TableCell>
                          <TableCell sx={{ fontFamily: "monospace" }}>
                            {token.token_prefix}…
                          </TableCell>
                          <TableCell>
                            {dateStringToLocalTime(token.created_at)}
                          </TableCell>
                          <TableCell>
                            {dateStringToLocalTime(token.expires_at) ?? "Never"}
                          </TableCell>
                          <TableCell>
                            {dateStringToLocalTime(token.last_used_at) ??
                              "Never"}
                          </TableCell>
                          {canModifyUsers && (
                            <TableCell align="right">
                              <Tooltip
                                title={
                                  username
                                    ? "Revoke"
                                    : "Cannot revoke: owner unknown"
                                }
                              >
                                <span>
                                  <IconButton
                                    size="small"
                                    disabled={!username}
                                    onClick={() => setTokenToRevoke(token)}
                                  >
                                    <DeleteIcon fontSize="small" />
                                  </IconButton>
                                </span>
                              </Tooltip>
                            </TableCell>
                          )}
                        </TableRow>
                      );
                    })}
                  </TableBody>
                </Table>
              </TableContainer>
              <TablePagination
                component="div"
                count={filtered.length}
                page={page}
                onPageChange={(_e, newPage) => setPage(newPage)}
                rowsPerPage={rowsPerPage}
                onRowsPerPageChange={(e) => {
                  setRowsPerPage(parseInt(e.target.value, 10));
                  setPage(0);
                }}
                rowsPerPageOptions={[10, 25, 50, 100]}
              />
            </>
          ))}
      </Paper>

      {canModifyUsers && (
        <>
          <CreateApiKeyDialog
            open={createOpen}
            onClose={() => setCreateOpen(false)}
            queryKey={ALL_TOKENS_QUERY_KEY}
            users={users ?? []}
            existingNamesFor={(user) =>
              (tokens ?? [])
                .filter((t) => user?.id !== undefined && t.user_id === user.id)
                .map((t) => t.name)
            }
          />
          <DeleteApiKeyDialog
            token={tokenToRevoke}
            onClose={() => setTokenToRevoke(null)}
            queryKey={ALL_TOKENS_QUERY_KEY}
            username={revokeOwner ?? undefined}
          />
        </>
      )}
    </Box>
  );
};

export default ApiKeyAdmin;
