// src/layouts/MainLayout.tsx
import {Outlet} from "react-router-dom";
import NavDrawer from '../components/NavDrawer';

import {Box, CssBaseline} from '@mui/material';

const MainLayout = () => {
    return (
        <Box sx={{ display: 'flex' }}>
            <CssBaseline />
            <NavDrawer />
            <Box
                component="main"
                sx={{
                    flexGrow: 1,
                    bgcolor: 'background.default',
                    p: 3,
                }}
            >
                <Outlet />
            </Box>
        </Box>
    );
};

export default MainLayout;