import Chip from "@mui/material/Chip";
import Stack from "@mui/material/Stack";
import { Typography } from "@mui/material";
import { useAuth } from "../Auth.tsx";

export default function ServerStatus() {
  const { serverStatus, serverInfo } = useAuth();
  const { authorized, userInfo } = useAuth();

  return (
    <Stack>
      {serverStatus == "loading" && (
        <>
          <Chip color="info" label="Loading" />
        </>
      )}
      {serverStatus == "connected" && (
        <>
          <Chip color="success" label="Connected" />

          {authorized ? (
            <Chip color="success" label="Authorized" />
          ) : (
            <Chip color="warning" label="Not authorized" />
          )}
          {userInfo ? (
            <Chip color="success" label={userInfo?.username} />
          ) : (
            <Chip color="warning" label="Anonymous" />
          )}

          <Stack>
            <Typography variant={"body1"}>{serverInfo.name}</Typography>
            <Typography variant={"body2"}>{serverInfo.version}</Typography>
          </Stack>
        </>
      )}
      {serverStatus == "disconnected" && (
        <>
          <Chip color="error" label="Not Connected" />
        </>
      )}
    </Stack>
  );
}
