import React, { useState } from "react";
import { usePageTitle } from "../UsePageTitle.ts";
import { useParams, useNavigate } from "react-router-dom";
import {
  Alert,
  Autocomplete,
  Avatar,
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
  MenuItem,
  Paper,
  Select,
  Stack,
  Switch,
  TextField,
  Typography,
} from "@mui/material";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { usePortalClient } from "../PortalClient";
import { useAuth } from "../Auth";
import * as qcpTypes from "../PortalTypes";
import LoadingIndicator from "../components/LoadingIndicator";
import ErrorIndicator from "../components/ErrorIndicator";
import { RoleChip } from "../components/RoleChip";

// ─── Utilities ────────────────────────────────────────────────────────────────

function getInitials(fullname?: string, username?: string): string {
  if (fullname) {
    const parts = fullname.trim().split(/\s+/);
    if (parts.length >= 2) return (parts[0][0] + parts[parts.length - 1][0]).toUpperCase();
    return parts[0][0]?.toUpperCase() ?? "?";
  }
  return (username ?? "?").slice(0, 2).toUpperCase();
}

// ─── Row primitives ───────────────────────────────────────────────────────────

const ROW_LABEL_WIDTH = 140;

const FieldRow: React.FC<{
  label: string;
  children: React.ReactNode;
  action?: React.ReactNode;
}> = ({ label, children, action }) => (
  <Box sx={{ display: "flex", alignItems: "center", px: 2.5, py: 1.5, minHeight: 52 }}>
    <Typography
      variant="body2"
      color="text.secondary"
      sx={{ width: ROW_LABEL_WIDTH, flexShrink: 0, fontWeight: 500 }}
    >
      {label}
    </Typography>
    <Box sx={{ flex: 1 }}>{children}</Box>
    {action && <Box sx={{ ml: 2, flexShrink: 0 }}>{action}</Box>}
  </Box>
);

// A text input row used while the page is in edit mode
const EditTextRow: React.FC<{
  label: string;
  value: string;
  onChange: (val: string) => void;
  error?: string;
  helperText?: string;
  autoFocus?: boolean;
}> = ({ label, value, onChange, error, helperText, autoFocus }) => (
  <Box sx={{ display: "flex", alignItems: "flex-start", px: 2.5, py: 1.25, minHeight: 52 }}>
    <Typography
      variant="body2"
      color="text.secondary"
      sx={{ width: ROW_LABEL_WIDTH, flexShrink: 0, fontWeight: 500, mt: 1 }}
    >
      {label}
    </Typography>
    <TextField
      value={value}
      onChange={(e) => onChange(e.target.value)}
      size="small"
      autoFocus={autoFocus}
      error={!!error}
      helperText={error ?? helperText ?? " "}
      sx={{ maxWidth: 320 }}
    />
  </Box>
);

// Inline change-password form (expands below the Password row)
const ChangePasswordForm: React.FC<{ username: string; onClose: () => void }> = ({ username, onClose }) => {
  const { makeRequest } = usePortalClient();
  const [current, setCurrent] = useState("");
  const [next, setNext] = useState("");
  const [confirm, setConfirm] = useState("");
  const [currentError, setCurrentError] = useState<string | undefined>();
  const [success, setSuccess] = useState(false);

  const mismatch = next.length > 0 && confirm.length > 0 && next !== confirm;
  const sameAsCurrent = current.length > 0 && next.length > 0 && next === current;
  const canSubmit = current.length > 0 && next.length > 0 && next === confirm && !sameAsCurrent;

  const mutation = useMutation<void, Error, string>({
    mutationFn: async (pw) => {
      // Verify current password against the login endpoint before changing
      try {
        await makeRequest<unknown>("POST", "auth/v1/session_login", { username, password: current });
      } catch {
        throw new Error("Current password is incorrect");
      }
      await makeRequest<void>("PUT", "api/v1/me/password", pw as unknown as object);
    },
    onSuccess: () => {
      setSuccess(true);
      setCurrent("");
      setNext("");
      setConfirm("");
      setCurrentError(undefined);
    },
    onError: (err) => {
      if (err.message === "Current password is incorrect") {
        setCurrentError(err.message);
      }
    },
  });

  const handleSubmit = () => {
    setCurrentError(undefined);
    mutation.mutate(next);
  };

  return (
    <Box sx={{ px: 2.5, pb: 2, pt: 0 }}>
      <Box sx={{ borderRadius: 1.5, bgcolor: "action.hover", p: 2, maxWidth: 380 }}>
        <Stack spacing={1.5}>
          {success && (
            <Alert severity="success" onClose={() => { setSuccess(false); onClose(); }}>
              Password changed successfully.
            </Alert>
          )}
          {mutation.isError && !currentError && (
            <Alert severity="error">{mutation.error.message}</Alert>
          )}
          <TextField
            label="Current password"
            type="password"
            value={current}
            onChange={(e) => { setCurrent(e.target.value); setCurrentError(undefined); }}
            size="small"
            fullWidth
            autoComplete="current-password"
            error={!!currentError}
            helperText={currentError ?? " "}
          />
          <TextField
            label="New password"
            type="password"
            value={next}
            onChange={(e) => setNext(e.target.value)}
            size="small"
            fullWidth
            autoComplete="new-password"
            error={sameAsCurrent}
            helperText={sameAsCurrent ? "New password must differ from current password" : " "}
          />
          <TextField
            label="Confirm new password"
            type="password"
            value={confirm}
            onChange={(e) => setConfirm(e.target.value)}
            size="small"
            fullWidth
            autoComplete="new-password"
            error={mismatch}
            helperText={mismatch ? "Passwords do not match" : " "}
          />
          <Stack direction="row" spacing={1}>
            <Button
              variant="contained"
              size="small"
              disabled={!canSubmit || mutation.isPending}
              onClick={handleSubmit}
            >
              {mutation.isPending ? <CircularProgress size={14} /> : "Change password"}
            </Button>
            <Button size="small" onClick={onClose} disabled={mutation.isPending}>
              Cancel
            </Button>
          </Stack>
        </Stack>
      </Box>
    </Box>
  );
};

// ─── Role select row (admin-only) ─────────────────────────────────────────────

const ROLE_OPTIONS = ["admin", "maintain", "monitor", "submit", "read"];

type Draft = {
  fullname: string;
  email: string;
  organization: string;
  role: string;
  enabled: boolean;
  groups: string[];
};

// ─── Section label ────────────────────────────────────────────────────────────

const SectionLabel: React.FC<{ children: string }> = ({ children }) => (
  <Typography
    variant="overline"
    color="text.disabled"
    sx={{ fontSize: "0.68rem", letterSpacing: 1.4, ml: 0.5 }}
  >
    {children}
  </Typography>
);

// ─── Main page ────────────────────────────────────────────────────────────────

const BaseUserInfo: React.FC<{ userName?: string }> = ({ userName }) => {
  const { makeRequest } = usePortalClient();
  const { userInfo, ping, has_permission } = useAuth();

  const pageTitle = userName ? `User Profile: ${userName}` : "Your Profile";
  usePageTitle(pageTitle);

  const queryClient = useQueryClient();
  const navigate = useNavigate();

  const isOwnProfile = !userName || userInfo?.username === userName;
  const canManageUsers = has_permission("users", "modify");
  const endpoint = isOwnProfile ? "api/v1/me" : `api/v1/users/${userName}`;

  const { status, data: userData, error } = useQuery({
    queryKey: ["userInfo", userName ?? "me"],
    queryFn: () => makeRequest<qcpTypes.UserInfo>("GET", endpoint),
  });

  const [showPasswordForm, setShowPasswordForm] = useState(false);
  const [deleteDialogOpen, setDeleteDialogOpen] = useState(false);

  // Page-level edit mode. `draft` holds the working copy while editing.
  const [draft, setDraft] = useState<Draft | null>(null);
  const [saving, setSaving] = useState(false);
  const [saveError, setSaveError] = useState<string | undefined>();
  const editing = draft !== null;

  const patchMutation = useMutation<void, Error, qcpTypes.UserInfo>({
    mutationFn: (body) => makeRequest<void>("PATCH", canManageUsers ? "api/v1/users" : "api/v1/me", body),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["userInfo", userName ?? "me"] });
      queryClient.invalidateQueries({ queryKey: ["listUsers"] });
      if (isOwnProfile) ping();
    },
  });

  const deleteMutation = useMutation<void, Error, void>({
    mutationFn: () => makeRequest<void>("DELETE", `api/v1/users/${userData?.username}`),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["listUsers"] });
      navigate("/users");
    },
  });

  // Available groups (admin-only) to populate the groups editor.
  const { data: allGroups } = useQuery({
    queryKey: ["listGroups"],
    queryFn: () => makeRequest<{ groupname: string }[]>("GET", "api/v1/groups"),
    enabled: canManageUsers,
  });

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
  if (!userData) return null;

  // Everyone can edit their own profile; admins can edit anyone's.
  const canEdit = isOwnProfile || canManageUsers;
  // Full name is the one field admins may not change on another user's profile.
  const canEditName = isOwnProfile;

  const startEditing = () => {
    setSaveError(undefined);
    setDraft({
      fullname: userData.fullname ?? "",
      email: userData.email ?? "",
      organization: userData.organization ?? "",
      role: userData.role,
      enabled: userData.enabled,
      groups: userData.groups,
    });
  };

  const cancelEditing = () => {
    setDraft(null);
  };

  const emailError =
    editing && draft!.email.length > 0 && !/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(draft!.email)
      ? "Enter a valid email address"
      : undefined;
  const saveBlocked = saving || !!emailError;

  const handleSave = async () => {
    if (!draft || saveBlocked) return;
    setSaving(true);
    setSaveError(undefined);
    try {
      // A group must exist before it can be assigned. Create any group the admin
      // typed that isn't already a known group.
      if (canManageUsers) {
        const existing = new Set(allGroups?.map((g) => g.groupname) ?? []);
        const newGroups = draft.groups.filter((g) => !existing.has(g));
        for (const groupname of newGroups) {
          await makeRequest("POST", "api/v1/groups", { groupname });
        }
        if (newGroups.length > 0) {
          queryClient.invalidateQueries({ queryKey: ["listGroups"] });
        }
      }
      await patchMutation.mutateAsync({
        id: userData.id,
        // Username is immutable; always send the existing value.
        username: userData.username,
        // Only admins may change role/enabled; otherwise keep the existing values.
        role: canManageUsers ? draft.role : userData.role,
        enabled: canManageUsers ? draft.enabled : userData.enabled,
        // Only admins may change group membership.
        groups: canManageUsers ? draft.groups : userData.groups,
        auth_type: userData.auth_type,
        // Only the owner may change their own full name.
        fullname: canEditName ? draft.fullname || undefined : userData.fullname,
        organization: draft.organization || undefined,
        email: draft.email || undefined,
      });
      setDraft(null);
    } catch (err) {
      setSaveError(err instanceof Error ? err.message : "Failed to save changes");
    } finally {
      setSaving(false);
    }
  };

  const initials = getInitials(userData.fullname, userData.username);

  return (
    <Box sx={{ width: "100%" }}>
      {/* ── Hero — full-bleed, breaks out of MainLayout Stack's mx: 3 ── */}
      <Box
        sx={{
          background: "linear-gradient(135deg, #0d3540 0%, #101c3a 60%, #090e1f 100%)",
          mx: -3,
          px: 7,
          pt: 4,
          pb: 5,
        }}
      >
        <Stack direction="row" spacing={2.5} alignItems="flex-end">
          <Box sx={{ position: "relative", flexShrink: 0 }}>
            <Avatar
              sx={{ width: 88, height: 88, fontSize: "1.9rem", bgcolor: "primary.main" }}
            >
              {initials}
            </Avatar>
          </Box>
          <Box>
            <Stack direction="row" spacing={1} alignItems="center" mb={0.5}>
              <Typography variant="h5" fontWeight="bold" sx={{ color: "#fff" }}>
                {userData.fullname || userData.username}
              </Typography>
              <RoleChip role={userData.role} />
            </Stack>
            <Typography variant="body2" sx={{ color: "rgba(255,255,255,0.6)" }}>
              @{userData.username}
              {userData.email ? ` · ${userData.email}` : ""}
            </Typography>
          </Box>
        </Stack>
      </Box>

      {/* ── Content ── */}
      <Box sx={{ pt: 3, pb: 5 }}>
        {/* EDIT TOOLBAR */}
        {canEdit && (
          <Stack direction="row" justifyContent="flex-end" alignItems="center" spacing={1} mb={2}>
            {editing ? (
              <>
                <Button
                  variant="contained"
                  size="small"
                  onClick={handleSave}
                  disabled={saveBlocked}
                  sx={{ minWidth: 72 }}
                >
                  {saving ? <CircularProgress size={16} /> : "Save"}
                </Button>
                <Button size="small" onClick={cancelEditing} disabled={saving}>
                  Cancel
                </Button>
              </>
            ) : (
              <Button variant="outlined" size="small" onClick={startEditing}>
                Edit
              </Button>
            )}
          </Stack>
        )}
        {editing && saveError && (
          <Alert severity="error" sx={{ mb: 2 }}>
            {saveError}
          </Alert>
        )}

        {/* PERSONAL */}
        <SectionLabel>Personal</SectionLabel>
        <Paper variant="outlined" sx={{ mt: 0.75, mb: 3 }}>
          <Stack divider={<Divider />}>
            {editing && canEditName ? (
              <EditTextRow
                label="Full name"
                value={draft!.fullname}
                onChange={(val) => setDraft({ ...draft!, fullname: val })}
                autoFocus
              />
            ) : (
              <FieldRow label="Full name">
                <Typography variant="body2">{userData.fullname || "—"}</Typography>
              </FieldRow>
            )}
            {/* Username is the immutable identity key the modify endpoint keys on;
                it cannot be renamed, so it is always read-only. */}
            <FieldRow label="Username">
              <Typography variant="body2">{userData.username}</Typography>
            </FieldRow>
            {editing ? (
              <EditTextRow
                label="Email"
                value={draft!.email}
                onChange={(val) => setDraft({ ...draft!, email: val })}
                error={emailError}
              />
            ) : (
              <FieldRow label="Email">
                <Typography variant="body2">{userData.email || "—"}</Typography>
              </FieldRow>
            )}
            {editing ? (
              <EditTextRow
                label="Organization"
                value={draft!.organization}
                onChange={(val) => setDraft({ ...draft!, organization: val })}
              />
            ) : (
              <FieldRow label="Organization">
                <Typography variant="body2">{userData.organization || "—"}</Typography>
              </FieldRow>
            )}
          </Stack>
        </Paper>

        {/* ACCOUNT */}
        <SectionLabel>Account</SectionLabel>
        <Paper variant="outlined" sx={{ mt: 0.75 }}>
          <Stack divider={<Divider />}>
            {editing && canManageUsers ? (
              <Box sx={{ display: "flex", alignItems: "center", px: 2.5, py: 1.25, minHeight: 52 }}>
                <Typography
                  variant="body2"
                  color="text.secondary"
                  sx={{ width: ROW_LABEL_WIDTH, flexShrink: 0, fontWeight: 500 }}
                >
                  Role
                </Typography>
                <Select
                  value={draft!.role}
                  onChange={(e) => setDraft({ ...draft!, role: e.target.value })}
                  size="small"
                  sx={{ minWidth: 160 }}
                >
                  {ROLE_OPTIONS.map((r) => (
                    <MenuItem key={r} value={r}><RoleChip role={r} /></MenuItem>
                  ))}
                </Select>
              </Box>
            ) : (
              <FieldRow label="Role">
                <Stack direction="row" spacing={1} alignItems="center">
                  <RoleChip role={userData.role} />
                  {!canManageUsers && (
                    <Typography variant="caption" color="text.disabled">
                      managed by your admin
                    </Typography>
                  )}
                </Stack>
              </FieldRow>
            )}

            {canManageUsers && (
              <FieldRow label="Enabled">
                <Stack direction="row" spacing={1} alignItems="center">
                  {editing ? (
                    <>
                      <Switch
                        checked={draft!.enabled}
                        onChange={(e) => setDraft({ ...draft!, enabled: e.target.checked })}
                        size="small"
                      />
                      <Typography variant="body2" color="text.secondary">
                        {draft!.enabled ? "Active" : "Disabled"}
                      </Typography>
                    </>
                  ) : (
                    <Chip
                      label={userData.enabled ? "Active" : "Disabled"}
                      size="small"
                      color={userData.enabled ? "success" : "default"}
                      variant="outlined"
                    />
                  )}
                </Stack>
              </FieldRow>
            )}

            <FieldRow label="Groups">
              {editing && canManageUsers ? (
                <Autocomplete
                  multiple
                  size="small"
                  freeSolo
                  autoSelect
                  selectOnFocus
                  clearOnBlur
                  handleHomeEndKeys
                  options={(allGroups?.map((g) => g.groupname) ?? []).filter(
                    (g) => !draft!.groups.includes(g),
                  )}
                  value={draft!.groups}
                  onChange={(_e, value) =>
                    setDraft({
                      ...draft!,
                      // Trim, drop blanks, and de-duplicate typed/selected names.
                      groups: Array.from(
                        new Set(value.map((v) => v.trim()).filter(Boolean)),
                      ),
                    })
                  }
                  renderInput={(params) => (
                    <TextField {...params} placeholder="Select or type to add a group" />
                  )}
                  sx={{ maxWidth: 360 }}
                />
              ) : userData.groups.length === 0 ? (
                <Typography variant="body2" color="text.secondary">—</Typography>
              ) : (
                <Stack direction="row" spacing={0.5} flexWrap="wrap">
                  {userData.groups.map((g) => (
                    <Chip key={g} label={g} size="small" variant="outlined" />
                  ))}
                </Stack>
              )}
            </FieldRow>

            {isOwnProfile && userData.auth_type === "password" && (
              <Box>
                <FieldRow
                  label="Password"
                  action={
                    !showPasswordForm ? (
                      <Button
                        size="small"
                        variant="text"
                        onClick={() => setShowPasswordForm(true)}
                      >
                        Change password
                      </Button>
                    ) : undefined
                  }
                >
                  <Typography variant="body2" color="text.secondary">
                    {showPasswordForm ? "Enter your new password below" : "••••••••"}
                  </Typography>
                </FieldRow>
                {showPasswordForm && (
                  <ChangePasswordForm username={userData.username} onClose={() => setShowPasswordForm(false)} />
                )}
              </Box>
            )}

            <FieldRow label="Authentication">
              <Chip label={userData.auth_type} size="small" variant="outlined" />
            </FieldRow>
          </Stack>
        </Paper>
        {canManageUsers && (
          <>
            <SectionLabel>Danger Zone</SectionLabel>
            <Paper
              variant="outlined"
              sx={{ mt: 0.75, borderColor: "error.main" }}
            >
              <FieldRow
                label="Delete user"
                action={
                  <Button
                    size="small"
                    color="error"
                    variant="outlined"
                    onClick={() => setDeleteDialogOpen(true)}
                  >
                    Delete
                  </Button>
                }
              >
                <Typography variant="body2" color="text.secondary">
                  Permanently remove this account.
                </Typography>
              </FieldRow>
            </Paper>

            <Dialog open={deleteDialogOpen} onClose={() => setDeleteDialogOpen(false)}>
              <DialogTitle>Delete user?</DialogTitle>
              <DialogContent>
                <DialogContentText>
                  This will permanently delete <strong>{userData.username}</strong>. This action cannot be undone.
                </DialogContentText>
              </DialogContent>
              <DialogActions>
                <Button onClick={() => setDeleteDialogOpen(false)} disabled={deleteMutation.isPending}>
                  Cancel
                </Button>
                <Button
                  color="error"
                  variant="contained"
                  onClick={() => deleteMutation.mutate()}
                  disabled={deleteMutation.isPending}
                >
                  {deleteMutation.isPending ? <CircularProgress size={14} /> : "Delete"}
                </Button>
              </DialogActions>
            </Dialog>
          </>
        )}
      </Box>
    </Box>
  );
};

const UserProfile: React.FC = () => {
  const { userName } = useParams();
  return <BaseUserInfo userName={userName} />;
};

export { UserProfile };
