import { usePortalClientAuth } from "../usePortalClient";
import Chip from '@mui/material/Chip';
import Stack from '@mui/material/Stack';
import {Typography} from "@mui/material";

export default function ServerStatus() {
    const { connectionState, serverInfo } = usePortalClientAuth();

    return (
        <Stack>
            {connectionState.connected ? (
                <>
                    <Chip color="success" label="Connected" />
                    <Stack>
                        <Typography variant={"body1"}>{serverInfo.name}</Typography>
                        <Typography variant={"body2"}>{serverInfo.version}</Typography>
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