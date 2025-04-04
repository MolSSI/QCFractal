// src/layouts/MainLayout.tsx
import {Outlet} from "react-router-dom";
import NavDrawer from '../components/NavDrawer';

import {Box, CssBaseline} from '@mui/material';

const MainLayout = () => {
    return (
        <Box sx={{ display: 'flex', height: '100vh' }}>
            <CssBaseline />
            <NavDrawer />
            <Box
                component="main"
                display="flex"
                sx={{
                    flexGrow: 1,
                    overflow: 'auto',
                    p: 2
                }}
            >
                <Outlet />
            </Box>
        </Box>
    );
};

export default MainLayout;