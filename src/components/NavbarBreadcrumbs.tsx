import { styled } from "@mui/material/styles";
import Typography from "@mui/material/Typography";
import MuiLink from "@mui/material/Link";
import Breadcrumbs, { breadcrumbsClasses } from "@mui/material/Breadcrumbs";
import NavigateNextRoundedIcon from "@mui/icons-material/NavigateNextRounded";
import { useLocation } from "react-router-dom";

const StyledBreadcrumbs = styled(Breadcrumbs)(({ theme }) => ({
  margin: theme.spacing(1, 0),
  [`& .${breadcrumbsClasses.separator}`]: {
    color: (theme.vars || theme).palette.action.disabled,
    margin: 1,
  },
  [`& .${breadcrumbsClasses.ol}`]: {
    alignItems: "center",
  },
}));

export default function NavbarBreadcrumbs() {
  const location = useLocation();

  // Split the current path into segments
  const pathnames = location.pathname.split("/").filter((x) => x);

  return (
    <StyledBreadcrumbs
      aria-label="breadcrumb"
      separator={<NavigateNextRoundedIcon fontSize="small" />}
    >
      {/* Always include a link to the Home */}
      <MuiLink to="/" variant="body1" sx={{ color: "inherit" }}>
        Home
      </MuiLink>

      {/* Dynamically generate breadcrumbs */}
      {pathnames.map((value, index) => {
        const to = `/${pathnames.slice(0, index + 1).join("/")}`;
        const isLast = index === pathnames.length - 1;

        let label = "";

        if (value === "projects") {
          label = "Projects";
        } else if (value === "records") {
          const next = pathnames[index + 1];
          if (next && /^\d+$/.test(next)) {
            return null; // 👈 Skip this breadcrumb if followed by an ID
          }
          label = "Records";
        } else if (pathnames[index - 1] === "projects") {
          label = `Project ${value}`;
        } else if (pathnames[index - 1] === "records") {
          label = `Record ${value}`;
        } else if (value === "datasets") {
          const next = pathnames[index + 1];
          if (next && /^\d+$/.test(next)) {
            return null; // 👈 Skip this breadcrumb if followed by an ID
          }
          label = "Datasets";
        } else if (pathnames[index - 1] === "datasets") {
          label = `Dataset ${value}`;
        } else {
          label = value.charAt(0).toUpperCase() + value.slice(1);
        }

        return isLast ? (
          <Typography
            key={to}
            variant="body1"
            sx={{
              color: "text.primary",
              fontWeight: 600,
            }}
          >
            {label}
          </Typography>
        ) : (
          <MuiLink key={to} to={to} variant="body1" sx={{ color: "inherit" }}>
            {label}
          </MuiLink>
        );
      })}
    </StyledBreadcrumbs>
  );
}
