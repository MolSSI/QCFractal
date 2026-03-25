import { useState } from "react";
import { Outlet } from "react-router-dom";
import { alpha } from "@mui/material/styles";
import SideMenu from "../components/SideMenu";
import { AppBar, Box, IconButton, Stack, Toolbar } from "@mui/material";
import MenuIcon from "@mui/icons-material/Menu";
import Header from "../components/Header";

function MainLayout() {
  const [mobileOpen, setMobileOpen] = useState(false);

  return (
    <Box sx={{ display: "flex", height: "100vh" }}>
      {/* Mobile top AppBar with hamburger button */}
      <AppBar
        position="fixed"
        sx={{
          display: { xs: "flex", md: "none" },
          zIndex: (theme) => theme.zIndex.drawer + 1,
        }}
      >
        <Toolbar>
          <IconButton
            color="inherit"
            edge="start"
            onClick={() => setMobileOpen((prev) => !prev)}
            aria-label="open navigation menu"
          >
            <MenuIcon />
          </IconButton>
        </Toolbar>
      </AppBar>

      <SideMenu
        mobileOpen={mobileOpen}
        onMobileClose={() => setMobileOpen(false)}
      />

      <Box
        component="main"
        sx={(theme) => ({
          flexGrow: 1,
          display: "flex",
          justifyContent: "center",
          mx: "auto",
          backgroundColor: theme.vars
            ? `rgba(${theme.vars.palette.background.defaultChannel} / 1)`
            : alpha(theme.palette.background.default, 1),
          overflow: "auto",
        })}
      >
        <Box
          sx={{
            width: "80%", // Ensures all content takes 70% of the right side
            mx: "auto", // Centers horizontally
          }}
        >
          <Stack
            spacing={2}
            sx={{
              alignItems: "center",
              pb: 5,
              mt: { xs: 8, md: 0 },
              mx: 3,
            }}
          >
            <Header />
            <Outlet />
          </Stack>
        </Box>
      </Box>
    </Box>
  );
}

export default MainLayout;
