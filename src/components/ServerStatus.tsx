import { usePortalClientAuth } from "../usePortalClient";
import Chip from '@mui/material/Chip';
import Stack from '@mui/material/Stack';
import {Typography} from "@mui/material";
import {useNavigate} from "react-router-dom";

export default function ServerStatus() {
    const { connectionState, serverInfo, logout } = usePortalClientAuth();
    const navigate = useNavigate();
    const connected = connectionState && connectionState.connected;
    const loading = !connectionState
    const error = !loading && !connected

    return (
        <Stack>
            {loading &&
                <>
                    <Chip color="info" label="Loading" />
                </>
            }
            {connected &&
                <>
                    <Chip color="success" label="Connected" />

                    {
                        connectionState!.authorized ? (
                            <Chip color="success" label="Authorized" />
                        ) : (
                            <Chip color="warning" label="Not authorized" />
                        )
                    }
                    {
                        connectionState!.userInfo ? (
                            <Chip color="success" label={connectionState?.userInfo?.username} />
                        ) : (
                            <Chip color="warning" label="Anonymous" />
                        )
                    }

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
            }
            { error &&
                <>
                    <Chip color="error" label="Not Connected" />
                </>
            }
        </Stack>
    );
}