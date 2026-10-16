import { Theme, alpha, Components } from "@mui/material/styles";
import { gray, orange } from "../themePrimitives";

/* eslint-disable import/prefer-default-export */
export const feedbackCustomizations: Components<Theme> = {
  MuiAlert: {
    styleOverrides: {
      root: ({ theme }) => ({
        borderRadius: (theme.vars || theme).shape.borderRadius,
        border: "1px solid transparent",
        color: (theme.vars || theme).palette.text.primary,
        "&.MuiAlert-standardWarning": {
          backgroundColor: orange[100],
          borderColor: alpha(orange[300], 0.5),
          "& .MuiAlert-icon": { color: orange[500] },
          ...theme.applyStyles("dark", {
            backgroundColor: alpha(orange[900], 0.5),
            borderColor: alpha(orange[800], 0.5),
          }),
        },
      }),
    },
  },
  MuiDialog: {
    styleOverrides: {
      root: ({ theme }) => ({
        "& .MuiDialog-paper": {
          borderRadius: (theme.vars || theme).shape.borderRadius,
          border: "1px solid",
          borderColor: (theme.vars || theme).palette.divider,
        },
      }),
    },
  },
  MuiDialogContentText: {
    styleOverrides: {
      root: ({ theme }) => ({
        fontSize: theme.typography.pxToRem(14),
        color: (theme.vars || theme).palette.text.primary,
      }),
    },
  },
  MuiLinearProgress: {
    styleOverrides: {
      root: ({ theme }) => ({
        height: 8,
        borderRadius: 8,
        backgroundColor: gray[200],
        ...theme.applyStyles("dark", {
          backgroundColor: gray[800],
        }),
      }),
    },
  },
};
