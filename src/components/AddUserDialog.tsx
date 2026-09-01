import React, { useState } from "react";
import {
  Alert,
  Autocomplete,
  Box,
  Button,
  CircularProgress,
  Dialog,
  DialogActions,
  DialogContent,
  DialogTitle,
  FormControl,
  FormControlLabel,
  FormHelperText,
  IconButton,
  InputAdornment,
  InputLabel,
  MenuItem,
  Select,
  Switch,
  TextField,
  Tooltip,
  Typography,
} from "@mui/material";
import ContentCopyIcon from "@mui/icons-material/ContentCopy";
import CheckIcon from "@mui/icons-material/Check";
import RefreshIcon from "@mui/icons-material/Refresh";
import { useMutation, useQueryClient } from "@tanstack/react-query";
import { usePortalClient } from "../PortalClient";
import * as qcpTypes from "../PortalTypes";
import global_role_permissions from "../global_role_permissions.json";

// Roles come from the same permission matrix used by useAuth(); "anonymous"
// is the implicit not-logged-in role and cannot be assigned to a real user.
const ASSIGNABLE_ROLES = Object.keys(global_role_permissions).filter(
  (r) => r !== "anonymous",
);

// Unambiguous charset (no 0/O, 1/l/I) so the password survives being read
// aloud or hand-copied.
const PASSWORD_CHARSET =
  "ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnpqrstuvwxyz23456789!@#$%^&*-_=+";

function generatePassword(length = 16): string {
  const values = new Uint32Array(length);
  crypto.getRandomValues(values);
  return Array.from(
    values,
    (v) => PASSWORD_CHARSET[v % PASSWORD_CHARSET.length],
  ).join("");
}

const EMAIL_RE = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;

interface AddUserDialogProps {
  open: boolean;
  onClose: () => void;
  existingUsernames: Set<string>;
  groups: qcpTypes.GroupInfo[];
  onCreated: (username: string) => void;
}

const AddUserDialog: React.FC<AddUserDialogProps> = ({
  open,
  onClose,
  existingUsernames,
  groups,
  onCreated,
}) => {
  const { makeRequest } = usePortalClient();
  const queryClient = useQueryClient();

  const [username, setUsername] = useState("");
  const [role, setRole] = useState("");
  const [enabled, setEnabled] = useState(true);
  const [fullname, setFullname] = useState("");
  const [email, setEmail] = useState("");
  const [organization, setOrganization] = useState("");
  const [memberGroups, setMemberGroups] = useState<string[]>([]);
  const [password, setPassword] = useState(() => generatePassword());
  const [copied, setCopied] = useState(false);
  // Set once the user has been created; switches the dialog to the
  // "save this password" view.
  const [createdUsername, setCreatedUsername] = useState<string | undefined>();

  const trimmedUsername = username.trim();
  const duplicate =
    trimmedUsername !== "" &&
    existingUsernames.has(trimmedUsername.toLowerCase());
  const emailInvalid = email.trim() !== "" && !EMAIL_RE.test(email.trim());
  const formValid = trimmedUsername !== "" && !duplicate && role !== "" && !emailInvalid;

  const resetAndClose = () => {
    setUsername("");
    setRole("");
    setEnabled(true);
    setFullname("");
    setEmail("");
    setOrganization("");
    setMemberGroups([]);
    setPassword(generatePassword());
    setCopied(false);
    setCreatedUsername(undefined);
    addMutation.reset();
    onClose();
  };

  const addMutation = useMutation({
    mutationFn: async () => {
      // A group must exist before it can be assigned. Create any group the
      // admin typed that isn't already a known group.
      const existing = new Set(groups.map((g) => g.groupname));
      const newGroups = memberGroups.filter((g) => !existing.has(g));
      for (const groupname of newGroups) {
        await makeRequest("POST", "api/v1/groups", { groupname });
      }
      if (newGroups.length > 0) {
        queryClient.invalidateQueries({ queryKey: ["listGroups"] });
      }
      const userInfo: qcpTypes.UserInfo = {
        username: trimmedUsername,
        role,
        enabled,
        auth_type: "password",
        groups: memberGroups,
        fullname: fullname.trim(),
        email: email.trim(),
        organization: organization.trim(),
      };
      // The backend expects a [UserInfo, password] tuple.
      return makeRequest<string | null>("POST", "api/v1/users", [
        userInfo,
        password,
      ]);
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["listUsers"] });
      setCreatedUsername(trimmedUsername);
      onCreated(trimmedUsername);
    },
  });

  const copyPassword = async () => {
    try {
      await navigator.clipboard.writeText(password);
      setCopied(true);
      setTimeout(() => setCopied(false), 2000);
    } catch {
      // Clipboard access can be denied; the password is still visible to
      // select and copy manually.
    }
  };

  const passwordField = (
    <TextField
      label="Generated password"
      value={password}
      fullWidth
      slotProps={{
        input: {
          readOnly: true,
          sx: { fontFamily: "monospace" },
          endAdornment: (
            <InputAdornment position="end">
              {!createdUsername && (
                <Tooltip title="Generate a new password">
                  <IconButton
                    size="small"
                    onClick={() => setPassword(generatePassword())}
                  >
                    <RefreshIcon fontSize="small" />
                  </IconButton>
                </Tooltip>
              )}
              <Tooltip title={copied ? "Copied!" : "Copy to clipboard"}>
                <IconButton size="small" onClick={copyPassword}>
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

  return (
    <Dialog
      open={open}
      onClose={createdUsername || addMutation.isPending ? undefined : resetAndClose}
      maxWidth="sm"
      fullWidth
    >
      {createdUsername ? (
        <>
          <DialogTitle>User Created</DialogTitle>
          <DialogContent>
            <Alert severity="success" sx={{ mb: 2 }}>
              User <strong>{createdUsername}</strong> was created successfully.
            </Alert>
            <Typography variant="body2" sx={{ mb: 2 }}>
              Share this password with the user now — it cannot be retrieved
              later. It can be reset from the user's profile page.
            </Typography>
            {passwordField}
          </DialogContent>
          <DialogActions>
            <Button variant="contained" onClick={resetAndClose}>
              Done
            </Button>
          </DialogActions>
        </>
      ) : (
        <>
          <DialogTitle>Add New User</DialogTitle>
          <DialogContent>
            <Typography variant="body2" color="text.secondary" sx={{ mb: 2 }}>
              Fields marked with * are required. A random password is generated
              for the new account.
            </Typography>
            <Box sx={{ display: "flex", flexDirection: "column", gap: 2, pt: 0.5 }}>
              <TextField
                label="Username"
                required
                autoFocus
                fullWidth
                value={username}
                onChange={(e) => setUsername(e.target.value)}
                error={duplicate}
                helperText={
                  duplicate ? "A user with this username already exists" : undefined
                }
              />
              <FormControl required fullWidth>
                <InputLabel id="add-user-role-label">Role</InputLabel>
                <Select
                  labelId="add-user-role-label"
                  label="Role *"
                  value={role}
                  onChange={(e) => setRole(e.target.value)}
                >
                  {ASSIGNABLE_ROLES.map((r) => (
                    <MenuItem key={r} value={r}>
                      {r}
                    </MenuItem>
                  ))}
                </Select>
                <FormHelperText>
                  Determines what the user is allowed to do on the server
                </FormHelperText>
              </FormControl>
              <FormControlLabel
                control={
                  <Switch
                    checked={enabled}
                    onChange={(e) => setEnabled(e.target.checked)}
                  />
                }
                label="Account enabled"
              />
              <TextField
                label="Full name (optional)"
                fullWidth
                value={fullname}
                onChange={(e) => setFullname(e.target.value)}
              />
              <TextField
                label="Email (optional)"
                fullWidth
                value={email}
                onChange={(e) => setEmail(e.target.value)}
                error={emailInvalid}
                helperText={emailInvalid ? "Enter a valid email address" : undefined}
              />
              <TextField
                label="Organization (optional)"
                fullWidth
                value={organization}
                onChange={(e) => setOrganization(e.target.value)}
              />
              <Autocomplete
                multiple
                freeSolo
                autoSelect
                selectOnFocus
                clearOnBlur
                handleHomeEndKeys
                options={groups
                  .map((g) => g.groupname)
                  .filter((g) => !memberGroups.includes(g))}
                value={memberGroups}
                onChange={(_e, value) =>
                  setMemberGroups(
                    // Trim, drop blanks, and de-duplicate typed/selected names.
                    Array.from(
                      new Set(value.map((v) => v.trim()).filter(Boolean)),
                    ),
                  )
                }
                renderInput={(params) => (
                  <TextField
                    {...params}
                    label="Groups (optional)"
                    helperText="Select existing groups, or type a new name and press Enter — the group will be created and the user assigned to it"
                  />
                )}
              />
              {passwordField}
              {addMutation.isError && (
                <Alert severity="error">
                  {addMutation.error instanceof Error
                    ? addMutation.error.message
                    : "Failed to create user"}
                </Alert>
              )}
            </Box>
          </DialogContent>
          <DialogActions>
            <Button onClick={resetAndClose} disabled={addMutation.isPending}>
              Cancel
            </Button>
            <Button
              variant="contained"
              disabled={!formValid || addMutation.isPending}
              onClick={() => addMutation.mutate()}
            >
              {addMutation.isPending ? (
                <CircularProgress size={22} color="inherit" />
              ) : (
                "Create User"
              )}
            </Button>
          </DialogActions>
        </>
      )}
    </Dialog>
  );
};

export { AddUserDialog };
