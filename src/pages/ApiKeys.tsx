import React, { useState } from "react";
import {
  Box,
  Button,
  Chip,
  Divider,
  IconButton,
  Paper,
  Stack,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  Tooltip,
  Typography,
} from "@mui/material";
import AddIcon from "@mui/icons-material/Add";
import DeleteIcon from "@mui/icons-material/Delete";
import EditIcon from "@mui/icons-material/Edit";
import { useQuery } from "@tanstack/react-query";
import * as qcpTypes from "../PortalTypes";
import { usePortalClient } from "../PortalClient.tsx";
import { usePageTitle } from "../UsePageTitle.ts";
import { dateStringToLocalTime, describeRequestError } from "../Utils.ts";
import LoadingIndicator from "../components/LoadingIndicator";
import ErrorIndicator from "../components/ErrorIndicator";
import {
  CreateApiKeyDialog,
  DeleteApiKeyDialog,
  RenameApiKeyDialog,
  isExpired,
} from "../components/ApiKeyDialogs.tsx";

const TOKENS_QUERY_KEY = ["myApiTokens"];

const ApiKeys: React.FC = () => {
  const { makeRequest } = usePortalClient();
  usePageTitle("API Keys");

  const [createOpen, setCreateOpen] = useState(false);
  const [tokenToDelete, setTokenToDelete] = useState<qcpTypes.APIToken | null>(
    null,
  );
  const [tokenToRename, setTokenToRename] = useState<qcpTypes.APIToken | null>(
    null,
  );

  const {
    status,
    data: tokens,
    error,
  } = useQuery({
    queryKey: TOKENS_QUERY_KEY,
    queryFn: () => makeRequest<qcpTypes.APIToken[]>("GET", "api/v1/me/tokens"),
  });

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
            API Keys
          </Typography>
          <Typography variant="body1" color="text.secondary">
            Long-lived keys that let scripts and clients authenticate as you
            without a password. Each key carries the permissions of your role.
          </Typography>
        </Box>
        <Button
          variant="contained"
          startIcon={<AddIcon />}
          onClick={() => setCreateOpen(true)}
          sx={{ flexShrink: 0 }}
        >
          Create API key
        </Button>
      </Stack>

      <Paper sx={{ p: 3, mt: 3 }}>
        <Typography variant="h6" gutterBottom>
          Your keys
        </Typography>
        <Divider sx={{ mb: 2 }} />

        {status === "pending" && <LoadingIndicator message="Loading keys..." />}
        {status === "error" && (
          <ErrorIndicator
            message={describeRequestError(error, "Failed to load API keys")}
          />
        )}
        {status === "success" &&
          (tokens.length === 0 ? (
            <Typography variant="body2" color="text.secondary" sx={{ py: 2 }}>
              You have no API keys yet.
            </Typography>
          ) : (
            <TableContainer>
              <Table size="small">
                <TableHead>
                  <TableRow>
                    <TableCell>Name</TableCell>
                    <TableCell>Prefix</TableCell>
                    <TableCell>Created</TableCell>
                    <TableCell>Expires</TableCell>
                    <TableCell>Last used</TableCell>
                    <TableCell align="right">Actions</TableCell>
                  </TableRow>
                </TableHead>
                <TableBody>
                  {tokens.map((token) => (
                    <TableRow key={token.id} hover>
                      <TableCell>
                        <Stack
                          direction="row"
                          spacing={1}
                          alignItems="center"
                          flexWrap="wrap"
                        >
                          <Typography variant="body2">{token.name}</Typography>
                          {isExpired(token) && (
                            <Chip
                              label="Expired"
                              size="small"
                              color="error"
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
                        {dateStringToLocalTime(token.last_used_at) ?? "Never"}
                      </TableCell>
                      <TableCell align="right">
                        <Tooltip title="Rename">
                          <IconButton
                            size="small"
                            onClick={() => setTokenToRename(token)}
                          >
                            <EditIcon fontSize="small" />
                          </IconButton>
                        </Tooltip>
                        <Tooltip title="Delete">
                          <IconButton
                            size="small"
                            onClick={() => setTokenToDelete(token)}
                          >
                            <DeleteIcon fontSize="small" />
                          </IconButton>
                        </Tooltip>
                      </TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            </TableContainer>
          ))}
      </Paper>

      <CreateApiKeyDialog
        open={createOpen}
        onClose={() => setCreateOpen(false)}
        queryKey={TOKENS_QUERY_KEY}
        existingNamesFor={() => (tokens ?? []).map((t) => t.name)}
      />
      <RenameApiKeyDialog
        token={tokenToRename}
        onClose={() => setTokenToRename(null)}
        queryKey={TOKENS_QUERY_KEY}
        existingNames={(tokens ?? []).map((t) => t.name)}
      />
      <DeleteApiKeyDialog
        token={tokenToDelete}
        onClose={() => setTokenToDelete(null)}
        queryKey={TOKENS_QUERY_KEY}
      />
    </Box>
  );
};

export default ApiKeys;
