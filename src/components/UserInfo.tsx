import { usePortalClient } from "../PortalClient.tsx";
import { useAuth } from "../Auth.tsx";
import React from "react";
import * as qcpTypes from "../PortalTypes";
import { useParams } from "react-router-dom";
import { Chip, Grid, Stack, Typography } from "@mui/material";
import { useQuery } from "@tanstack/react-query";

const BaseUserInfo: React.FC<{ userName?: string }> = ({ userName }) => {
  const { makeRequest } = usePortalClient();
  const { userInfo } = useAuth();

  const {
    status,
    data: userData,
    error,
  } = useQuery({
    queryKey: ["userInfo", userName ?? "me"],
    queryFn: () =>
      makeRequest<qcpTypes.UserInfo>(
        "GET",
        userName ? `api/v1/users/${userName}` : `api/v1/me`,
      ),
  });

  const isAdmin = userInfo?.role == "admin";
  const isThisUser =
    !userName || userInfo?.username == userName;

  return (
    <>
      {status === "pending" && <Typography>Loading...</Typography>}
      {status === "error" && (
        <Typography color="error">
          Error loading user: {error.message}
        </Typography>
      )}

      {status === "success" && userData && (
        <Grid container spacing={2} width="100%">
          <Grid size={12}>
            <Stack spacing={1}>
              <Typography variant="h4" fontWeight="bold">
                {userData.username}
              </Typography>
              <Chip
                label={userData.enabled ? "Enabled" : "Disabled"}
                color={userData.enabled ? "success" : "warning"}
                sx={{ width: "fit-content", fontWeight: "bold" }}
              />
            </Stack>
          </Grid>
          <Grid size={3}>
            <Typography variant="body1">Role: {userData.role}</Typography>

            {(isAdmin || isThisUser) && (
              <Typography variant="body1">
                Auth: {userData.auth_type}
              </Typography>
            )}
          </Grid>
          <Grid size={4}>
            <Typography variant="h6">Groups</Typography>
            {userData.groups.length == 0 ? (
              <Typography variant="body1">(no groups)</Typography>
            ) : (
              <ul>
                {userData.groups.map((group, index) => (
                  <li key={index}>{group}</li>
                ))}
              </ul>
            )}
          </Grid>
        </Grid>
      )}
    </>
  );
};

const UserInfo: React.FC = () => {
  const { userName } = useParams();
  return <BaseUserInfo userName={userName} />;
};

export { UserInfo };
