import { styled, useColorScheme } from "@mui/material/styles";
import Avatar from "@mui/material/Avatar";
import MuiDrawer, { drawerClasses } from "@mui/material/Drawer";
import Box from "@mui/material/Box";
import Stack from "@mui/material/Stack";
import Typography from "@mui/material/Typography";
import MenuContent from "./MenuContent";
import OptionsMenu from "./OptionsMenu";
import Button from "@mui/material/Button";
import ServerStatus from "../components/ServerStatus";
import { usePortalClientAuth } from "../usePortalClient";
import { useNavigate } from "react-router-dom";
import qcarchiveLogo from "../assets/qcarchive_logo.svg";
import qcarchiveLogoInverted from "../assets/qcarchive_logo_inverted.svg";

const drawerWidth = 240;

const Drawer = styled(MuiDrawer)({
  width: drawerWidth,
  flexShrink: 0,
  boxSizing: "border-box",
  mt: 10,
  [`& .${drawerClasses.paper}`]: {
    width: drawerWidth,
    boxSizing: "border-box",
  },
});

export default function SideMenu() {
  const { mode, systemMode, setMode } = useColorScheme();
  const resolvedMode = (systemMode || mode) as 'light' | 'dark';
  const { connectionState, logout } = usePortalClientAuth();
  const navigate = useNavigate();

  return (
    <Drawer
      variant="permanent"
      sx={{
        display: { xs: "none", md: "block" },
        [`& .${drawerClasses.paper}`]: {
          backgroundColor: "background.paper",
        },
      }}
    >
      {/* Logo Section */}
      <Box
        sx={{
          display: "flex",
          justifyContent: "center",
          alignItems: "center",
          p: 1,
          borderBottom: "1px solid",
          borderColor: "divider",
        }}
      >
        <Avatar
          src={resolvedMode == "dark" ? qcarchiveLogoInverted: qcarchiveLogo}
          alt="QCArchive Logo"
          sx={{ width: 125, height: 100 }}
          variant="square"
        />
      </Box>
      <Box
        sx={{
          overflow: "auto",
          height: "100%",
          display: "flex",
          flexDirection: "column",
        }}
      >
        <MenuContent />
        <ServerStatus />
      </Box>
      <Stack
        direction="row"
        sx={{
          p: 2,
          gap: 1,
          alignItems: "center",
          borderTop: "1px solid",
          borderColor: "divider",
        }}
      >
        {connectionState?.userInfo ? (
          <>
            <Avatar
              sizes="small"
              alt={connectionState.userInfo.username}
              sx={{ width: 36, height: 36 }}
            />
            <Box sx={{ mr: "auto" }}>
              <Typography
                variant="body2"
                sx={{ fontWeight: 500, lineHeight: "16px" }}
              >
                {connectionState.userInfo.username}
              </Typography>
              <Typography variant="caption" sx={{ color: "text.secondary" }}>
                {connectionState.userInfo.role}
              </Typography>
            </Box>
            <OptionsMenu />
          </>
        ) : (
          <>
            <Avatar
              sizes="small"
              alt="(Not logged in)"
              sx={{ width: 36, height: 36 }}
            >
              ?
            </Avatar>
            <Stack sx={{ mr: "auto" }}>
              <Typography
                variant="body2"
                sx={{ fontWeight: 500, lineHeight: "16px", p: 2 }}
              >
                (Not logged in)
              </Typography>
              <Button
                variant="contained"
                color="primary"
                size="small"
                onClick={() => {
                  logout(true);
                  navigate(
                    `/login?redirect=${encodeURIComponent(location.pathname + location.search)}`,
                  );
                }}
              >
                Login
              </Button>
            </Stack>
          </>
        )}
      </Stack>
    </Drawer>
  );
}
