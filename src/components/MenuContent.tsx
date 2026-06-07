import { ReactElement } from "react";
import List from "@mui/material/List";
import ListItem from "@mui/material/ListItem";
import ListItemButton from "@mui/material/ListItemButton";
import ListItemIcon from "@mui/material/ListItemIcon";
import ListItemText from "@mui/material/ListItemText";
import Stack from "@mui/material/Stack";
import HomeRoundedIcon from "@mui/icons-material/HomeRounded";
import FormatListBulletedIcon from "@mui/icons-material/FormatListBulleted";
import AccountTreeIcon from "@mui/icons-material/AccountTree";
//import CarpenterIcon from "@mui/icons-material/Carpenter";
import ComputerIcon from "@mui/icons-material/Computer";
import WorkHistoryIcon from "@mui/icons-material/WorkHistory";
import BugReportIcon from "@mui/icons-material/BugReport";
import PeopleRoundedIcon from "@mui/icons-material/PeopleRounded";
import QueryStatsIcon from "@mui/icons-material/QueryStats";
import CodeIcon from "@mui/icons-material/Code";
import { NavLink, useLocation } from "react-router-dom";
import FeedbackIcon from "@mui/icons-material/Feedback";
import { useAuth } from "../Auth.tsx";

const feedbackUrl: string = import.meta.env.VITE_FEEDBACK_URL;

const mainListItems: {
  text: string;
  icon: ReactElement;
  path: string;
  target?: string;
  requiredPermissions?: [string, string];
}[] = [
  { text: "Home", icon: <HomeRoundedIcon />, path: "/" },
  { text: "Projects", icon: <AccountTreeIcon />, path: "/projects" },
  { text: "Datasets", icon: <FormatListBulletedIcon />, path: "/datasets" },
  { text: "Compute", icon: <ComputerIcon />, path: "/managers" },
  {
    text: "Users",
    icon: <PeopleRoundedIcon />,
    path: "/users",
    requiredPermissions: ["users", "read"],
  },
  {
    text: "Internal Jobs",
    icon: <WorkHistoryIcon />,
    path: "/internal_jobs",
    requiredPermissions: ["internal_jobs", "read"],
  },
  { text: "Server Stats", icon: <QueryStatsIcon />, path: "/server_stats" },
  { text: "API Access", icon: <CodeIcon />, path: "/api_access" },
  {
    text: "Server Errors",
    icon: <BugReportIcon />,
    path: "/server_errors",
    requiredPermissions: ["server_errors", "read"],
  },
];

const bottomListItems: {
  text: string;
  icon: ReactElement;
  path: string;
  target?: string;
}[] = [
  //  { text: "Sandbox", icon: <CarpenterIcon />, path: "/sandbox" },
  //  { text: "Theme Playground", icon: <CarpenterIcon />, path: "/themeplayground" },
];

if (feedbackUrl) {
  mainListItems.push({
    text: "Feedback",
    icon: <FeedbackIcon />,
    path: feedbackUrl,
    target: "_blank",
  });
}

interface MenuContentProps {
  onNavigate?: () => void;
}

export default function MenuContent({ onNavigate }: MenuContentProps) {
  const location = useLocation();
  const { has_permission } = useAuth();

  const filteredMainListItems = mainListItems.filter((item) => {
    if (item.requiredPermissions) {
      const [resource, action] = item.requiredPermissions;
      return has_permission(resource, action);
    }
    return true;
  });

  return (
    <Stack sx={{ flexGrow: 1, p: 1, justifyContent: "space-between" }}>
      <List dense>
        {filteredMainListItems.map((item, index) => (
          <ListItem key={index} disablePadding sx={{ display: "block" }}>
            <ListItemButton
              component={NavLink}
              to={item.path}
              selected={location.pathname === item.path}
              target={item.target ? item.target : ""}
              onClick={onNavigate}
              sx={{ cursor: "pointer" }}
            >
              <ListItemIcon>{item.icon}</ListItemIcon>
              <ListItemText primary={item.text} />
            </ListItemButton>
          </ListItem>
        ))}
      </List>
      <List dense>
        {bottomListItems.map((item, index) => (
          <ListItem key={index} disablePadding sx={{ display: "block" }}>
            <ListItemButton
              component={NavLink}
              to={item.path}
              selected={location.pathname === item.path}
              target={item.target ? item.target : ""}
              onClick={onNavigate}
              sx={{ cursor: "pointer" }}
            >
              <ListItemIcon>{item.icon}</ListItemIcon>
              <ListItemText primary={item.text} />
            </ListItemButton>
          </ListItem>
        ))}
      </List>
    </Stack>
  );
}
