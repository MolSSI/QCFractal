import React, { useState } from "react";
import {
  Alert,
  Autocomplete,
  Button,
  Chip,
  CircularProgress,
  Dialog,
  DialogActions,
  DialogContent,
  DialogContentText,
  DialogTitle,
  IconButton,
  InputAdornment,
  MenuItem,
  Stack,
  TextField,
  Tooltip,
} from "@mui/material";
import CheckIcon from "@mui/icons-material/Check";
import ContentCopyIcon from "@mui/icons-material/ContentCopy";
import { useMutation, useQueryClient } from "@tanstack/react-query";
import * as qcpTypes from "../PortalTypes";
import { usePortalClient } from "../PortalClient.tsx";
import { describeRequestError } from "../Utils.ts";

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

export function isExpired(token: qcpTypes.APIToken): boolean {
  return !!token.expires_at && new Date(token.expires_at) <= new Date();
}

export function tokenBasePath(username?: string): string {
  return username ? `api/v1/users/${username}/tokens` : "api/v1/me/tokens";
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

export interface CreateApiKeyDialogProps {
  open: boolean;
  onClose: () => void;
  queryKey: unknown[];
  /**
   * Supplying users turns on the owner picker and targets the chosen user's
   * endpoint; leaving it out targets the signed-in user via /me.
   */
  users?: qcpTypes.UserInfo[];
  existingNamesFor: (user?: qcpTypes.UserInfo) => string[];
}

export const CreateApiKeyDialog: React.FC<CreateApiKeyDialogProps> = ({
  open,
  onClose,
  queryKey,
  users,
  existingNamesFor,
}) => {
  const { makeRequest } = usePortalClient();
  const queryClient = useQueryClient();

  const pickUser = users !== undefined;
  const [owner, setOwner] = useState<qcpTypes.UserInfo | null>(null);
  const [name, setName] = useState("");
  const [expirationDays, setExpirationDays] = useState<number | null>(90);
  const [newToken, setNewToken] = useState<qcpTypes.NewAPIToken | null>(null);

  const trimmedName = name.trim();
  const nameError = existingNamesFor(owner ?? undefined).includes(trimmedName)
    ? pickUser
      ? "This user already has a key with this name"
      : "You already have a key with this name"
    : undefined;

  const createMutation = useMutation<
    qcpTypes.NewAPIToken,
    Error,
    qcpTypes.APITokenCreateBody
  >({
    mutationFn: (body) =>
      makeRequest<qcpTypes.NewAPIToken>(
        "POST",
        tokenBasePath(pickUser ? owner?.username : undefined),
        body,
      ),
    onSuccess: (created) => {
      setNewToken(created);
      queryClient.invalidateQueries({ queryKey });
    },
  });

  const handleClose = () => {
    if (createMutation.isPending) return;
    onClose();
    // Reset after the close transition so the dialog doesn't flicker.
    setTimeout(() => {
      setOwner(null);
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

  const createdFor =
    newToken && users?.find((u) => u.id === newToken.info.user_id);

  return (
    <Dialog open={open} onClose={handleClose} maxWidth="sm" fullWidth>
      {newToken === null ? (
        <>
          <DialogTitle>
            {pickUser ? "Create API key for a user" : "Create API key"}
          </DialogTitle>
          <DialogContent>
            <DialogContentText sx={{ mb: 2 }}>
              {pickUser
                ? "The key authenticates as the chosen user and carries that user's permissions. It is shown only once, right after it is created, so you will need to pass it on to them securely."
                : "An API key lets a script or client authenticate as you, with the same permissions as your role. The key is shown only once, right after it is created."}
            </DialogContentText>
            <Stack spacing={2} sx={{ mt: 1 }}>
              {pickUser && (
                <Autocomplete
                  options={users}
                  value={owner}
                  onChange={(_e, value) => setOwner(value)}
                  getOptionLabel={(u) => u.username}
                  isOptionEqualToValue={(a, b) => a.username === b.username}
                  size="small"
                  renderInput={(params) => (
                    <TextField
                      {...params}
                      label="Owner"
                      autoFocus
                      helperText="The user this key will authenticate as"
                    />
                  )}
                />
              )}
              <TextField
                label="Name"
                value={name}
                onChange={(e) => setName(e.target.value)}
                size="small"
                fullWidth
                autoFocus={!pickUser}
                error={!!nameError}
                helperText={
                  nameError ?? "Something that identifies where the key is used"
                }
                slotProps={{ htmlInput: { maxLength: 128 } }}
              />
              <TextField
                label="Expiration"
                value={
                  expirationDays === null ? "never" : String(expirationDays)
                }
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
                !trimmedName ||
                !!nameError ||
                (pickUser && !owner) ||
                createMutation.isPending
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
          <DialogTitle>
            {pickUser
              ? `Copy the API key for ${createdFor?.username ?? "this user"}`
              : "Copy your API key"}
          </DialogTitle>
          <DialogContent>
            <Alert severity="warning" sx={{ mb: 2 }}>
              {pickUser
                ? "This is the only time the key will be shown. Hand it to the user over a secure channel — if it is lost, revoke it and create a new one."
                : "This is the only time the key will be shown. Store it somewhere safe — if you lose it, delete the key and create a new one."}
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

export interface RenameApiKeyDialogProps {
  token: qcpTypes.APIToken | null;
  onClose: () => void;
  queryKey: unknown[];
  /** Owner of the key, for the admin endpoint; omitted to rename via /me. */
  username?: string;
  /** The owner's other key names, so a clash is caught before the server rejects it. */
  existingNames: string[];
}

export const RenameApiKeyDialog: React.FC<RenameApiKeyDialogProps> = ({
  token,
  onClose,
  queryKey,
  username,
  existingNames,
}) => {
  const { makeRequest } = usePortalClient();
  const queryClient = useQueryClient();

  const [name, setName] = useState("");

  React.useEffect(() => {
    if (token) setName(token.name);
  }, [token]);

  const trimmedName = name.trim();
  const unchanged = trimmedName === token?.name;
  const nameError =
    !unchanged && existingNames.includes(trimmedName)
      ? username
        ? "This user already has a key with this name"
        : "You already have a key with this name"
      : undefined;

  const renameMutation = useMutation<
    qcpTypes.APIToken,
    Error,
    qcpTypes.APITokenModifyBody
  >({
    mutationFn: (body) =>
      makeRequest<qcpTypes.APIToken>(
        "PATCH",
        `${tokenBasePath(username)}/${token?.id}`,
        body,
      ),
    onSuccess: async () => {
      await queryClient.invalidateQueries({ queryKey });
      onClose();
      setTimeout(() => renameMutation.reset(), 200);
    },
  });

  const handleClose = () => {
    if (renameMutation.isPending) return;
    onClose();
    setTimeout(() => renameMutation.reset(), 200);
  };

  return (
    <Dialog open={!!token} onClose={handleClose} maxWidth="xs" fullWidth>
      <DialogTitle>Rename API key</DialogTitle>
      <DialogContent>
        <DialogContentText sx={{ mb: 2 }}>
          The name is the only part of a key that can be changed. Its secret,
          owner, scope and expiration are fixed when the key is created.
        </DialogContentText>
        <TextField
          label="Name"
          value={name}
          onChange={(e) => setName(e.target.value)}
          size="small"
          fullWidth
          autoFocus
          error={!!nameError}
          helperText={nameError ?? " "}
          slotProps={{ htmlInput: { maxLength: 128 } }}
        />
        {renameMutation.isError && (
          <Alert severity="error" sx={{ mt: 1 }}>
            {describeRequestError(
              renameMutation.error,
              "Failed to rename API key",
            )}
          </Alert>
        )}
      </DialogContent>
      <DialogActions>
        <Button onClick={handleClose} disabled={renameMutation.isPending}>
          Cancel
        </Button>
        <Button
          variant="contained"
          onClick={() => renameMutation.mutate({ name: trimmedName })}
          disabled={
            !trimmedName || !!nameError || unchanged || renameMutation.isPending
          }
        >
          {renameMutation.isPending ? <CircularProgress size={16} /> : "Rename"}
        </Button>
      </DialogActions>
    </Dialog>
  );
};

export interface DeleteApiKeyDialogProps {
  token: qcpTypes.APIToken | null;
  onClose: () => void;
  queryKey: unknown[];
  /** Owner of the key, for the admin endpoint; omitted to revoke via /me. */
  username?: string;
}

export const DeleteApiKeyDialog: React.FC<DeleteApiKeyDialogProps> = ({
  token,
  onClose,
  queryKey,
  username,
}) => {
  const { makeRequest } = usePortalClient();
  const queryClient = useQueryClient();

  const deleteMutation = useMutation<void, Error, number>({
    mutationFn: (tokenId) =>
      makeRequest<void>("DELETE", `${tokenBasePath(username)}/${tokenId}`),
    onSuccess: async () => {
      await queryClient.invalidateQueries({ queryKey });
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
      <DialogTitle>
        {username ? "Revoke API key?" : "Delete API key?"}
      </DialogTitle>
      <DialogContent>
        <DialogContentText>
          <strong>{token?.name}</strong>
          {username ? (
            <>
              , owned by <Chip label={username} size="small" sx={{ mx: 0.5 }} />
              , will stop working immediately.
            </>
          ) : (
            " will stop working immediately."
          )}{" "}
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
          {deleteMutation.isPending ? (
            <CircularProgress size={14} />
          ) : username ? (
            "Revoke"
          ) : (
            "Delete"
          )}
        </Button>
      </DialogActions>
    </Dialog>
  );
};
