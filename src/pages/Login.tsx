import React, { useState } from "react";
import AppTheme from "../shared-theme/AppTheme";
import {
  Box,
  Button,
  Card,
  CardContent,
  TextField,
  Typography,
} from "@mui/material";
import { useNavigate, useSearchParams } from "react-router-dom";
import { usePortalClientAuth } from "../usePortalClient";
import { alpha } from "@mui/material/styles";

const LoginPage: React.FC = () => {
  const [username, setUsername] = useState("");
  const [password, setPassword] = useState("");
  const [error, setError] = useState("");
  const { login } = usePortalClientAuth();
  const navigate = useNavigate();
  const [searchParams] = useSearchParams();

  const redirectTo = searchParams.get("redirect") || "/";

  const handleLogin = async (event: React.FormEvent) => {
    event.preventDefault();
    setError("");
    if (!username || !password) {
      setError("Both fields are required");
      return;
    }
    try {
      await login(username, password);
      navigate(redirectTo, { replace: true });
    } catch (err) {
      setError("Invalid credentials or login failed");
    }
  };

  return (
    <AppTheme>
      <Box
        sx={(theme) => ({
          width: "100vw",
          height: "100vh",
          backgroundColor: theme.vars
            ? `rgba(${theme.vars.palette.background.defaultChannel} / 1)`
            : alpha(theme.palette.background.default, 1),
          display: "flex",
          justifyContent: "center",
          alignItems: "center",
        })}
      >
        <Card sx={{ width: "300px", p: 2, boxShadow: 3 }}>
          <CardContent>
            <Typography variant="h5" gutterBottom textAlign="center">
              Login
            </Typography>
            {error && <Typography color="error">{error}</Typography>}
            <form onSubmit={handleLogin}>
              <TextField
                fullWidth
                label="Username"
                margin="normal"
                variant="outlined"
                value={username}
                onChange={(e) => setUsername(e.target.value)}
              />
              <TextField
                fullWidth
                label="Password"
                type="password"
                margin="normal"
                variant="outlined"
                value={password}
                onChange={(e) => setPassword(e.target.value)}
              />
              <Button
                fullWidth
                variant="contained"
                color="primary"
                type="submit"
                sx={{ mt: 2 }}
              >
                Login
              </Button>
            </form>
          </CardContent>
        </Card>
      </Box>
    </AppTheme>
  );
};

export default LoginPage;
