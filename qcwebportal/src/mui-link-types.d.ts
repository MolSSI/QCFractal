import type { To } from "react-router-dom";

declare module "@mui/material/Link" {
  interface LinkOwnProps {
    to?: To;
    replace?: boolean;
    state?: unknown;
  }
}
