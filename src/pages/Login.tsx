import React, { useState } from "react";
import { TextField, Button, Container, Box, Typography, Card, CardContent } from "@mui/material";
import { useNavigate } from "react-router-dom";
import { usePortalClientAuth } from "../usePortalClient";

const LoginPage: React.FC = () => {
    const [username, setUsername] = useState("");
    const [password, setPassword] = useState("");
    const [error, setError] = useState("");
    const {login} = usePortalClientAuth();
    const navigate = useNavigate();

    const handleLogin = async (event: React.FormEvent) => {
        event.preventDefault();
        setError("");
        if (!username || !password) {
            setError("Both fields are required");
            return;
        }
        try {
            await login(username, password);
            navigate("/");
        } catch (err) {
            setError("Invalid credentials or login failed");
        }
    };

    return (
        <Container maxWidth="xs">
            <Box display="flex" justifyContent="center" alignItems="center" minHeight="100vh">
                <Card sx={{ width: "100%", p: 2, boxShadow: 3 }}>
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
                            <Button fullWidth variant="contained" color="primary" type="submit" sx={{ mt: 2 }}>
                                Login
                            </Button>
                        </form>
                    </CardContent>
                </Card>
            </Box>
        </Container>
    );
};

export default LoginPage;
