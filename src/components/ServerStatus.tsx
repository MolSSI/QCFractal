import { usePortalClientAuth } from "../usePortalClient";
import Chip from '@mui/material/Chip';
import Stack from '@mui/material/Stack';
import {Typography} from "@mui/material";
import {useNavigate} from "react-router-dom";

export default function ServerStatus() {
    const { connectionState, serverInfo, logout } = usePortalClientAuth();
    const navigate = useNavigate();

    return (
        <Stack>
            {connectionState.connected ? (
                <>
                    <Chip color="success" label="Connected" />
                    <Stack>
                        <Typography variant={"body1"}>{serverInfo.name}</Typography>
                        <Typography variant={"body2"}>{serverInfo.version}</Typography>
                        <button onClick={() => {
                            logout(false);
                            navigate("/login");
                        }}>Logout
                        </button>
                    </Stack>
                </>
            ) : (
                <>
                    <Chip color="error" label="Not Connected" />
                </>
            )
            }
        </Stack>
    );
}