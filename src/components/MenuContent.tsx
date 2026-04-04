import List from "@mui/material/List";
import ListItem from "@mui/material/ListItem";
import ListItemButton from "@mui/material/ListItemButton";
import ListItemIcon from "@mui/material/ListItemIcon";
import ListItemText from "@mui/material/ListItemText";
import Stack from "@mui/material/Stack";
import HomeRoundedIcon from "@mui/icons-material/HomeRounded";
import FormatListBulletedIcon from "@mui/icons-material/FormatListBulleted";
import CarpenterIcon from "@mui/icons-material/Carpenter";
import ComputerIcon from "@mui/icons-material/Computer";
import { useNavigate, useLocation } from "react-router-dom";

const mainListItems = [
  { text: "Home", icon: <HomeRoundedIcon />, path: "/" },
  { text: "Projects", icon: <FormatListBulletedIcon />, path: "/projects" },
  { text: "Compute", icon: <ComputerIcon />, path: "/managers" },
  { text: "Sandbox", icon: <CarpenterIcon />, path: "/sandbox" },
];

interface MenuContentProps {
  onNavigate?: () => void;
}

export default function MenuContent({ onNavigate }: MenuContentProps) {
  const navigate = useNavigate();
  const location = useLocation();

  const handleClick = (path: string) => {
    navigate(path);
    onNavigate?.();
  };

  return (
    <Stack sx={{ flexGrow: 1, p: 1, justifyContent: "space-between" }}>
      <List dense>
        {mainListItems.map((item, index) => (
          <ListItem key={index} disablePadding sx={{ display: "block" }}>
            <ListItemButton
              selected={location.pathname === item.path}
              onClick={() => handleClick(item.path)}
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
