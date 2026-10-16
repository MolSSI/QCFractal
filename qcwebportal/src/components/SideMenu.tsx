import { styled } from "@mui/material/styles";
import Avatar from "@mui/material/Avatar";
import MuiDrawer, { drawerClasses } from "@mui/material/Drawer";
import Box from "@mui/material/Box";
import Stack from "@mui/material/Stack";
import Typography from "@mui/material/Typography";
import MenuContent from "./MenuContent";
import OptionsMenu from "./OptionsMenu";
import Button from "@mui/material/Button";
import ServerStatus from "../components/ServerStatus";
import { useAuth } from "../Auth.tsx";
import { Link, useLocation } from "react-router-dom";
import Chip from "@mui/material/Chip";
import qcarchiveLogo from "../assets/qcarchive_logo.svg";
import qcarchiveLogoInverted from "../assets/qcarchive_logo_inverted.svg";
import { useResolvedColorMode } from "../shared-theme/useResolvedColorMode";

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

interface SideMenuProps {
  mobileOpen?: boolean;
  onMobileClose?: () => void;
}

function DrawerContents({ onNavigate }: { onNavigate?: () => void }) {
  const resolvedMode = useResolvedColorMode();
  const { userInfo, logout } = useAuth();
  const location = useLocation();
  const loginPath = `/login?redirect=${encodeURIComponent(location.pathname + location.search)}`;

  return (
    <>
      {/* Logo Section */}
      <Box
        sx={{
          display: "flex",
          justifyContent: "center",
          alignItems: "center",
          p: 1,
          borderBottom: "1px solid",
          borderColor: "divider",
          flexDirection: "column",
          gap: 2,
        }}
      >
        <Avatar
          src={resolvedMode == "dark" ? qcarchiveLogoInverted : qcarchiveLogo}
          alt="QCArchive Logo"
          sx={{ width: 125, height: 100 }}
          variant="square"
        />
        <Chip label="ALPHA" variant="filled" color="warning" />
      </Box>
      <Box
        sx={{
          overflow: "auto",
          height: "100%",
          display: "flex",
          flexDirection: "column",
        }}
      >
        <MenuContent onNavigate={onNavigate} />
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
        {userInfo ? (
          <>
            <Avatar
              sizes="small"
              alt={userInfo.username}
              sx={{ width: 36, height: 36 }}
            />
            <Box sx={{ mr: "auto" }}>
              <Typography
                variant="body2"
                sx={{ fontWeight: 500, lineHeight: "16px" }}
              >
                {userInfo.username}
              </Typography>
              <Typography variant="caption" sx={{ color: "text.secondary" }}>
                {userInfo.role}
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
                component={Link}
                to={loginPath}
                onClick={logout}
              >
                Login
              </Button>
            </Stack>
          </>
        )}
      </Stack>
    </>
  );
}

export default function SideMenu({
  mobileOpen = false,
  onMobileClose,
}: SideMenuProps) {
  return (
    <>
      {/* Mobile: temporary drawer, toggled by hamburger button */}
      <MuiDrawer
        variant="temporary"
        open={mobileOpen}
        onClose={onMobileClose}
        ModalProps={{ keepMounted: true }}
        sx={{
          display: { xs: "block", md: "none" },
          [`& .${drawerClasses.paper}`]: {
            width: drawerWidth,
            boxSizing: "border-box",
            backgroundColor: "background.paper",
          },
        }}
      >
        <DrawerContents onNavigate={onMobileClose} />
      </MuiDrawer>

      {/* Desktop: permanent drawer */}
      <Drawer
        variant="permanent"
        sx={{
          display: { xs: "none", md: "block" },
          [`& .${drawerClasses.paper}`]: {
            backgroundColor: "background.paper",
          },
        }}
      >
        <DrawerContents />
      </Drawer>
    </>
  );
}
