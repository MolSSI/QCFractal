// src/pages/Profile.tsx
import { usePortalClientRequest } from "../usePortalClient";
import { FetchedData } from "../PortalClientContext";
import React, { useEffect, useState } from "react";
import * as qcpTypes from "../PortalTypes";
import { useParams } from "react-router-dom";
import {
  Box,
  Chip,
  Grid,
  Paper,
  Stack,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  Typography,
} from "@mui/material";
import { parseToDate } from "../Utils";

const BaseUserInfo: React.FC<{ userName?: string }> = ({ userName }) => {
  const { connectionState, fetchData } = usePortalClientRequest(); // Get client instance here

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

  const isAdmin = connectionState?.userInfo?.role == "admin";
  const isThisUser =
    !userName || connectionState?.userInfo?.username == userName;

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
          <Grid item size={12}>
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
            <Typography variant="body1">Auth: {userData.auth_type}</Typography>
          </Grid>
          <Grid item size={4}>
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

const MyUserInfo: React.FC = () => {
  return <BaseUserInfo />;
};
export { MyUserInfo, UserInfo };
