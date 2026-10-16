import { useColorScheme, useTheme } from "@mui/material/styles";

export type ResolvedColorMode = "light" | "dark";

export function useResolvedColorMode(): ResolvedColorMode {
  const theme = useTheme();
  const { mode, systemMode } = useColorScheme();

  if (systemMode) {
    return systemMode;
  }

  if (mode === "light" || mode === "dark") {
    return mode;
  }

  return theme.palette.mode === "dark" ? "dark" : "light";
}
