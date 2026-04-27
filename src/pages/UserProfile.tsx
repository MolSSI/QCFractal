import React, { useState, useEffect } from "react";
import { useParams } from "react-router-dom";
import {
  Alert,
  Avatar,
  Box,
  Button,
  Chip,
  CircularProgress,
  Divider,
  Paper,
  Stack,
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

// A row that can be toggled into an inline edit input
const EditableFieldRow: React.FC<{
  label: string;
  value: string | undefined;
  readOnly?: boolean;
  validate?: (val: string) => string | undefined;
  asyncValidate?: (val: string) => Promise<string | undefined>;
  onSave: (val: string) => Promise<void>;
}> = ({ label, value, readOnly, validate, asyncValidate, onSave }) => {
  const [editing, setEditing] = useState(false);
  const [draft, setDraft] = useState(value ?? "");
  const [saving, setSaving] = useState(false);
  const [asyncError, setAsyncError] = useState<string | undefined>();
  const [isChecking, setIsChecking] = useState(false);

  const syncError = editing && draft.length > 0 ? validate?.(draft) : undefined;
  const fieldError = syncError ?? asyncError;
  const blocked = saving || !!fieldError || isChecking;

  useEffect(() => {
    if (!asyncValidate || !editing || draft === (value ?? "")) {
      setAsyncError(undefined);
      setIsChecking(false);
      return;
    }
    setIsChecking(true);
    setAsyncError(undefined);
    const timer = setTimeout(async () => {
      const err = await asyncValidate(draft);
      setAsyncError(err);
      setIsChecking(false);
    }, 500);
    return () => clearTimeout(timer);
  }, [draft, asyncValidate, editing, value]);

  const handleSave = async () => {
    if (blocked) return;
    setSaving(true);
    try {
      await onSave(draft);
      setEditing(false);
    } finally {
      setSaving(false);
    }
  };

  const handleCancel = () => {
    setDraft(value ?? "");
    setAsyncError(undefined);
    setIsChecking(false);
    setEditing(false);
  };

  if (editing) {
    const helperText = fieldError ?? (isChecking ? "Checking…" : " ");
    return (
      <Box sx={{ display: "flex", alignItems: "center", px: 2.5, py: 1.25, gap: 1.5, minHeight: 52 }}>
        <Typography
          variant="body2"
          color="text.secondary"
          sx={{ width: ROW_LABEL_WIDTH, flexShrink: 0, fontWeight: 500 }}
        >
          {label}
        </Typography>
        <TextField
          value={draft}
          onChange={(e) => setDraft(e.target.value)}
          size="small"
          autoFocus
          error={!!fieldError}
          helperText={helperText}
          onKeyDown={(e) => { if (e.key === "Enter") handleSave(); if (e.key === "Escape") handleCancel(); }}
          sx={{ maxWidth: 280 }}
        />
        <Button size="small" variant="contained" onClick={handleSave} disabled={blocked} sx={{ minWidth: 56 }}>
          {saving ? <CircularProgress size={14} /> : "Save"}
        </Button>
        <Button size="small" onClick={handleCancel} disabled={saving}>
          Cancel
        </Button>
      </Box>
    );
  }

  return (
    <FieldRow
      label={label}
      action={
        !readOnly ? (
          <Button size="small" variant="text" onClick={() => setEditing(true)}>
            Edit
          </Button>
        ) : undefined
      }
    >
      <Typography variant="body2">{value || "—"}</Typography>
    </FieldRow>
  );
};

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
  const { userInfo, ping } = useAuth();
  const queryClient = useQueryClient();

  const isOwnProfile = !userName || userInfo?.username === userName;
  const endpoint = isOwnProfile ? "api/v1/me" : `api/v1/users/${userName}`;

  const { status, data: userData, error } = useQuery({
    queryKey: ["userInfo", userName ?? "me"],
    queryFn: () => makeRequest<qcpTypes.UserInfo>("GET", endpoint),
  });

  const [showPasswordForm, setShowPasswordForm] = useState(false);

  const patchMutation = useMutation<void, Error, qcpTypes.UserModifyBody>({
    mutationFn: (body) => makeRequest<void>("PATCH", "api/v1/me", body),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["userInfo", "me"] });
      ping();
    },
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

  const saveField = async (
    field: "fullname" | "organization" | "email" | "username",
    value: string,
  ) => {
    await patchMutation.mutateAsync({
      id: userData.id,
      username: field === "username" ? value : userData.username,
      role: userData.role,
      enabled: userData.enabled,
      groups: userData.groups,
      auth_type: userData.auth_type,
      fullname: field === "fullname" ? value || undefined : userData.fullname,
      organization: field === "organization" ? value || undefined : userData.organization,
      email: field === "email" ? value || undefined : userData.email,
    });
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
        {/* PERSONAL */}
        <SectionLabel>Personal</SectionLabel>
        <Paper variant="outlined" sx={{ mt: 0.75, mb: 3 }}>
          <Stack divider={<Divider />}>
            <EditableFieldRow
              label="Full name"
              value={userData.fullname}
              readOnly={!isOwnProfile}
              onSave={(val) => saveField("fullname", val)}
            />
            <EditableFieldRow
              label="Username"
              value={userData.username}
              readOnly={!isOwnProfile}
              asyncValidate={async (val) => {
                try {
                  await makeRequest("GET", `api/v1/users/${val}`);
                  return "Username is already taken";
                } catch {
                  return undefined;
                }
              }}
              onSave={(val) => saveField("username", val)}
            />
            <EditableFieldRow
              label="Email"
              value={userData.email}
              readOnly={!isOwnProfile}
              validate={(val) => /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(val) ? undefined : "Enter a valid email address"}
              onSave={(val) => saveField("email", val)}
            />
            <EditableFieldRow
              label="Organization"
              value={userData.organization}
              readOnly={!isOwnProfile}
              onSave={(val) => saveField("organization", val)}
            />
          </Stack>
        </Paper>

        {/* ACCOUNT */}
        <SectionLabel>Account</SectionLabel>
        <Paper variant="outlined" sx={{ mt: 0.75 }}>
          <Stack divider={<Divider />}>
            <FieldRow label="Role">
              <Stack direction="row" spacing={1} alignItems="center">
                <RoleChip role={userData.role} />
                <Typography variant="caption" color="text.disabled">
                  managed by your admin
                </Typography>
              </Stack>
            </FieldRow>

            <FieldRow label="Groups">
              {userData.groups.length === 0 ? (
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
      </Box>
    </Box>
  );
};

const UserProfile: React.FC = () => {
  const { userName } = useParams();
  return <BaseUserInfo userName={userName} />;
};

export { UserProfile };
