import React, { useState } from "react";
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
  Divider,
  IconButton,
  InputAdornment,
  MenuItem,
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
import AddIcon from "@mui/icons-material/Add";
import CheckIcon from "@mui/icons-material/Check";
import ContentCopyIcon from "@mui/icons-material/ContentCopy";
import DeleteIcon from "@mui/icons-material/Delete";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import * as qcpTypes from "../PortalTypes";
import { usePortalClient } from "../PortalClient.tsx";
import { usePageTitle } from "../UsePageTitle.ts";
import { dateStringToLocalTime, describeRequestError } from "../Utils.ts";
import LoadingIndicator from "../components/LoadingIndicator";
import ErrorIndicator from "../components/ErrorIndicator";

const TOKENS_QUERY_KEY = ["myApiTokens"];

const EXPIRATION_OPTIONS: { label: string; days: number | null }[] = [
  { label: "30 days", days: 30 },
  { label: "60 days", days: 60 },
  { label: "90 days", days: 90 },
  { label: "1 year", days: 365 },
  { label: "No expiration", days: null },
];

function expiresAtFromDays(days: number | null): string | null {
  if (days === null) {
    return null;
  }

  const expires = new Date();
  expires.setDate(expires.getDate() + days);
  return expires.toISOString();
}

function isExpired(token: qcpTypes.APIToken): boolean {
  return !!token.expires_at && new Date(token.expires_at) <= new Date();
}

const CopyableToken: React.FC<{ token: string }> = ({ token }) => {
  const [copied, setCopied] = useState(false);

  const handleCopy = async () => {
    try {
      await navigator.clipboard.writeText(token);
      setCopied(true);
      setTimeout(() => setCopied(false), 2000);
    } catch {
      // Clipboard is unavailable outside a secure context; the text is still
      // selectable in the field.
    }
  };

  return (
    <TextField
      value={token}
      fullWidth
      size="small"
      multiline
      slotProps={{
        input: {
          readOnly: true,
          sx: { fontFamily: "monospace", fontSize: "0.8rem" },
          endAdornment: (
            <InputAdornment position="end">
              <Tooltip title={copied ? "Copied" : "Copy"}>
                <IconButton onClick={handleCopy} edge="end" size="small">
                  {copied ? (
                    <CheckIcon fontSize="small" color="success" />
                  ) : (
                    <ContentCopyIcon fontSize="small" />
                  )}
                </IconButton>
              </Tooltip>
            </InputAdornment>
          ),
        },
      }}
    />
  );
};

const CreateTokenDialog: React.FC<{
  open: boolean;
  existingNames: string[];
  onClose: () => void;
}> = ({ open, existingNames, onClose }) => {
  const { makeRequest } = usePortalClient();
  const queryClient = useQueryClient();

  const [name, setName] = useState("");
  const [expirationDays, setExpirationDays] = useState<number | null>(90);
  const [newToken, setNewToken] = useState<qcpTypes.NewAPIToken | null>(null);

  const trimmedName = name.trim();
  const duplicateName = existingNames.includes(trimmedName);
  const nameError = duplicateName
    ? "You already have a key with this name"
    : undefined;

  const createMutation = useMutation<
    qcpTypes.NewAPIToken,
    Error,
    qcpTypes.APITokenCreateBody
  >({
    mutationFn: (body) =>
      makeRequest<qcpTypes.NewAPIToken>("POST", "api/v1/me/tokens", body),
    onSuccess: (created) => {
      setNewToken(created);
      queryClient.invalidateQueries({ queryKey: TOKENS_QUERY_KEY });
    },
  });

  const handleClose = () => {
    if (createMutation.isPending) return;
    onClose();
    // Reset after the close transition so the dialog doesn't flicker.
    setTimeout(() => {
      setName("");
      setExpirationDays(90);
      setNewToken(null);
      createMutation.reset();
    }, 200);
  };

  const handleCreate = () => {
    createMutation.mutate({
      name: trimmedName,
      expires_at: expiresAtFromDays(expirationDays),
    });
  };

  return (
    <Dialog open={open} onClose={handleClose} maxWidth="sm" fullWidth>
      {newToken === null ? (
        <>
          <DialogTitle>Create API key</DialogTitle>
          <DialogContent>
            <DialogContentText sx={{ mb: 2 }}>
              An API key lets a script or client authenticate as you, with the
              same permissions as your role. The key is shown only once, right
              after it is created.
            </DialogContentText>
            <Stack spacing={2} sx={{ mt: 1 }}>
              <TextField
                label="Name"
                value={name}
                onChange={(e) => setName(e.target.value)}
                size="small"
                fullWidth
                autoFocus
                error={!!nameError}
                helperText={
                  nameError ?? "Something that identifies where the key is used"
                }
                slotProps={{ htmlInput: { maxLength: 128 } }}
              />
              <TextField
                label="Expiration"
                value={expirationDays === null ? "never" : String(expirationDays)}
                onChange={(e) =>
                  setExpirationDays(
                    e.target.value === "never" ? null : Number(e.target.value),
                  )
                }
                select
                size="small"
                sx={{ maxWidth: 240 }}
              >
                {EXPIRATION_OPTIONS.map((opt) => (
                  <MenuItem
                    key={opt.label}
                    value={opt.days === null ? "never" : String(opt.days)}
                  >
                    {opt.label}
                  </MenuItem>
                ))}
              </TextField>
            </Stack>
            {createMutation.isError && (
              <Alert severity="error" sx={{ mt: 2 }}>
                {describeRequestError(
                  createMutation.error,
                  "Failed to create API key",
                )}
              </Alert>
            )}
          </DialogContent>
          <DialogActions>
            <Button onClick={handleClose} disabled={createMutation.isPending}>
              Cancel
            </Button>
            <Button
              variant="contained"
              onClick={handleCreate}
              disabled={
                !trimmedName || !!nameError || createMutation.isPending
              }
            >
              {createMutation.isPending ? (
                <CircularProgress size={16} />
              ) : (
                "Create key"
              )}
            </Button>
          </DialogActions>
        </>
      ) : (
        <>
          <DialogTitle>Copy your API key</DialogTitle>
          <DialogContent>
            <Alert severity="warning" sx={{ mb: 2 }}>
              This is the only time the key will be shown. Store it somewhere
              safe — if you lose it, delete the key and create a new one.
            </Alert>
            <CopyableToken token={newToken.token} />
          </DialogContent>
          <DialogActions>
            <Button variant="contained" onClick={handleClose}>
              Done
            </Button>
          </DialogActions>
        </>
      )}
    </Dialog>
  );
};

const DeleteTokenDialog: React.FC<{
  token: qcpTypes.APIToken | null;
  onClose: () => void;
}> = ({ token, onClose }) => {
  const { makeRequest } = usePortalClient();
  const queryClient = useQueryClient();

  const deleteMutation = useMutation<void, Error, number>({
    mutationFn: (tokenId) =>
      makeRequest<void>("DELETE", `api/v1/me/tokens/${tokenId}`),
    onSuccess: async () => {
      await queryClient.invalidateQueries({ queryKey: TOKENS_QUERY_KEY });
      onClose();
      setTimeout(() => deleteMutation.reset(), 200);
    },
  });

  const handleClose = () => {
    if (deleteMutation.isPending) return;
    onClose();
    setTimeout(() => deleteMutation.reset(), 200);
  };

  return (
    <Dialog open={!!token} onClose={handleClose} maxWidth="xs" fullWidth>
      <DialogTitle>Delete API key?</DialogTitle>
      <DialogContent>
        <DialogContentText>
          <strong>{token?.name}</strong> will stop working immediately.
          Anything still using it will fail to authenticate. This cannot be
          undone.
        </DialogContentText>
        {deleteMutation.isError && (
          <Alert severity="error" sx={{ mt: 2 }}>
            {describeRequestError(
              deleteMutation.error,
              "Failed to delete API key",
            )}
          </Alert>
        )}
      </DialogContent>
      <DialogActions>
        <Button onClick={handleClose} disabled={deleteMutation.isPending}>
          Cancel
        </Button>
        <Button
          color="error"
          variant="contained"
          onClick={() => token && deleteMutation.mutate(token.id)}
          disabled={deleteMutation.isPending}
        >
          {deleteMutation.isPending ? <CircularProgress size={14} /> : "Delete"}
        </Button>
      </DialogActions>
    </Dialog>
  );
};

const ApiKeys: React.FC = () => {
  const { makeRequest } = usePortalClient();
  usePageTitle("API Keys");

  const [createOpen, setCreateOpen] = useState(false);
  const [tokenToDelete, setTokenToDelete] = useState<qcpTypes.APIToken | null>(
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

      <CreateTokenDialog
        open={createOpen}
        existingNames={(tokens ?? []).map((t) => t.name)}
        onClose={() => setCreateOpen(false)}
      />
      <DeleteTokenDialog
        token={tokenToDelete}
        onClose={() => setTokenToDelete(null)}
      />
    </Box>
  );
};

export default ApiKeys;
