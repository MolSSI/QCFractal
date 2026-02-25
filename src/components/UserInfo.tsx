import { FetchedData, usePortalClient } from "../PortalClient.tsx";
import { useAuth } from "../Auth.tsx";
import React, { useEffect, useState } from "react";
import * as qcpTypes from "../PortalTypes";
import { useParams } from "react-router-dom";
import { Chip, Grid, Stack, Typography } from "@mui/material";

const BaseUserInfo: React.FC<{ userName?: string }> = ({ userName }) => {
  const { fetchData } = usePortalClient(); // Get client instance here
  const { userInfo } = useAuth();

  const [userInfoFetchedData, setUserInfoFetchedData] = useState<
    FetchedData<qcpTypes.UserInfo>
  >({
    data: undefined,
    error: undefined,
    loading: true,
  });

  // Load project data, then dataset & record metadata
  useEffect(() => {
    if (!userName) {
      fetchData<qcpTypes.UserInfo>(setUserInfoFetchedData, "get", `api/v1/me`);
    } else {
      fetchData<qcpTypes.UserInfo>(
        setUserInfoFetchedData,
        "get",
        `api/v1/users/${userName}`,
      );
    }
  }, [fetchData, userName]);

  const userData = userInfoFetchedData?.data;

  const isAdmin = userInfo?.role == "admin";
  const isThisUser =
    !userName || userInfo?.username == userName;

  return (
    <>
      {userInfoFetchedData.loading && <Typography>Loading...</Typography>}
      {userInfoFetchedData.error && (
        <Typography color="error">
          Error loading user: {userInfoFetchedData.error}
        </Typography>
      )}

      {!userInfoFetchedData.loading && userData && (
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
