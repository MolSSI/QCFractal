import Drawer from '@mui/material/Drawer';
import List from '@mui/material/List';
import Divider from '@mui/material/Divider';
import ListItem from '@mui/material/ListItem';
import ListItemButton from '@mui/material/ListItemButton';
import ListItemIcon from '@mui/material/ListItemIcon';
import ListItemText from '@mui/material/ListItemText';
import {useNavigate} from "react-router-dom";
import MailIcon from '@mui/icons-material/Mail';
import InboxIcon from '@mui/icons-material/MoveToInbox';
import ServerStatus from "./ServerStatus.tsx";
import FormatListBulletedIcon from '@mui/icons-material/FormatListBulleted';
const drawerWidth = 240;

export default function PermanentDrawerLeft() {
    const navigate = useNavigate();
    const handleClick = (text: string) => {
        if (text === "Home") {
            navigate("/");
        }
        else if (text === "Projects") {
            navigate("/projects");
        }
    }

    return (
        <Drawer
            sx={{
                width: drawerWidth,
                flexShrink: 0,
            }}
            variant="permanent"
            anchor="left"
        >
            <Divider />
            <ServerStatus />
            <Divider />
            <List>
                {['Home', 'Projects', 'Send email', 'Drafts'].map((text, index) => (
                    <ListItem key={text} disablePadding>
                        <ListItemButton onClick={() => handleClick(text)}>
                            <ListItemIcon>
                                {index % 2 === 0 ? <FormatListBulletedIcon /> : <MailIcon />}
                            </ListItemIcon>
                            <ListItemText primary={text} />
                        </ListItemButton>
                    </ListItem>
                ))}
            </List>
            <Divider />
            <List>
                {['All mail', 'Trash', 'Spam'].map((text, index) => (
                    <ListItem key={text} disablePadding>
                        <ListItemButton>
                            <ListItemIcon>
                                {index % 2 === 0 ? <InboxIcon /> : <MailIcon />}
                            </ListItemIcon>
                            <ListItemText primary={text} />
                        </ListItemButton>
                    </ListItem>
                ))}
            </List>
            <Divider />
        </Drawer>
    );
}