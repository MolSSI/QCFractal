import * as React from "react";
import {  useState } from "react";
import List from "@mui/material/List";
import ListItem from "@mui/material/ListItem";
import ListItemButton from "@mui/material/ListItemButton";
import ListItemIcon from "@mui/material/ListItemIcon";
import ListItemText from "@mui/material/ListItemText";
import Stack from "@mui/material/Stack";
import HomeRoundedIcon from "@mui/icons-material/HomeRounded";
import FormatListBulletedIcon from "@mui/icons-material/FormatListBulleted"
import PeopleRoundedIcon from "@mui/icons-material/PeopleRounded";
import ComputerIcon from "@mui/icons-material/Computer";
import SettingsRoundedIcon from "@mui/icons-material/SettingsRounded";
import InfoRoundedIcon from "@mui/icons-material/InfoRounded";
import HelpRoundedIcon from "@mui/icons-material/HelpRounded";
import {useNavigate} from "react-router-dom";

const mainListItems = [
  { text: "Home", icon: <HomeRoundedIcon />, path: "/" },
  { text: "Projects", icon: <FormatListBulletedIcon />, path: "/projects" },
  { text: "Clients", icon: <PeopleRoundedIcon />, path: "/" },
  { text: "Compute", icon: <ComputerIcon />, path: "/managers" },
];

const secondaryListItems = [
  { text: "Settings", icon: <SettingsRoundedIcon /> },
  { text: "About", icon: <InfoRoundedIcon /> },
  { text: "Feedback", icon: <HelpRoundedIcon /> },
];

export default function MenuContent() {
    const navigate = useNavigate();
    const [selectedIndex, setSelectedIndex] = useState(0);

    // interface HandleClick {
    //     (index: number, path: string): void;
    // }

    const handleClick = (index: number, path: string) => {
            setSelectedIndex(index);
            navigate(path);
    };

    return (
      <Stack sx={{ flexGrow: 1, p: 1, justifyContent: "space-between" }}>
        <List dense>
          {mainListItems.map((item, index) => (
            <ListItem key={index} disablePadding sx={{ display: "block" }}>
              <ListItemButton
                selected={selectedIndex === index}
                onClick={() => handleClick(index, item.path)}
                sx={{cursor: "pointer"}}
              >
                <ListItemIcon>{item.icon}</ListItemIcon>
                <ListItemText primary={item.text} />
              </ListItemButton>
            </ListItem>
          ))}
        </List>
        <List dense>
          {secondaryListItems.map((item, index) => (
            <ListItem key={index} disablePadding sx={{ display: "block" }}>
              <ListItemButton>
                <ListItemIcon>{item.icon}</ListItemIcon>
                <ListItemText primary={item.text} />
              </ListItemButton>
            </ListItem>
          ))}
        </List>
      </Stack>
    );
}
