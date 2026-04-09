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
import CarpenterIcon from "@mui/icons-material/Carpenter";
import ComputerIcon from "@mui/icons-material/Computer";
import { NavLink, useLocation } from "react-router-dom";
import FeedbackIcon from "@mui/icons-material/Feedback";

const feedbackUrl: string = import.meta.env.VITE_FEEDBACK_URL;

const mainListItems: {
  text: string;
  icon: ReactElement;
  path: string;
  target?: string;
}[] = [
  { text: "Home", icon: <HomeRoundedIcon />, path: "/" },
  { text: "Projects", icon: <AccountTreeIcon />, path: "/projects" },
  { text: "Datasets", icon: <FormatListBulletedIcon />, path: "/datasets" },
  { text: "Compute", icon: <ComputerIcon />, path: "/managers" },
  { text: "Sandbox", icon: <CarpenterIcon />, path: "/sandbox" },
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

  return (
    <Stack sx={{ flexGrow: 1, p: 1, justifyContent: "space-between" }}>
      <List dense>
        {mainListItems.map((item, index) => (
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
