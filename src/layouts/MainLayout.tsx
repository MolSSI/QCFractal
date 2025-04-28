import { Outlet } from "react-router-dom";
import { alpha } from "@mui/material/styles";
import SideMenu from "../components/SideMenu";
import { Box, CssBaseline, Stack } from "@mui/material";
import AppTheme from "../shared-theme/AppTheme";
import Header from "../components/Header";
import {
  chartsCustomizations,
  dataGridCustomizations,
  treeViewCustomizations,
} from "../theme/customizations";

const xThemeComponents = {
  ...chartsCustomizations,
  ...dataGridCustomizations,
  ...treeViewCustomizations,
};

function MainLayout() {
  return (
    <AppTheme themeComponents={xThemeComponents}>
      <CssBaseline enableColorScheme />
      <Box sx={{ display: "flex", height: "100vh" }}>
        <SideMenu />
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
    </AppTheme>
  );
}

export default MainLayout;
