import { styled } from "@mui/material/styles";
import Typography from "@mui/material/Typography";
import Breadcrumbs, { breadcrumbsClasses } from "@mui/material/Breadcrumbs";
import NavigateNextRoundedIcon from "@mui/icons-material/NavigateNextRounded";
import { Link, useLocation } from "react-router-dom";

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
      <Typography
        component={Link}
        to="/"
        variant="body1"
        sx={{
          textDecoration: "none",
          color: "inherit",
          "&:hover": { textDecoration: "underline" },
        }}
      >
        Home
      </Typography>

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
          label = "Project";
        } else if (pathnames[index - 1] === "records") {
          label = "Record";
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
          <Typography
            key={to}
            component={Link}
            to={to}
            variant="body1"
            sx={{
              textDecoration: "none",
              color: "inherit",
              "&:hover": { textDecoration: "underline" },
            }}
          >
            {label}
          </Typography>
        );
      })}
    </StyledBreadcrumbs>
  );
}
