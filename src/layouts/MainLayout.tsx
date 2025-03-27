// src/layouts/MainLayout.tsx
import React, { ReactNode } from 'react';
import NavDrawer from '../components/NavDrawer';

import { Box, CssBaseline } from '@mui/material';

interface MainLayoutProps {
    children: ReactNode;
}

const MainLayout: React.FC<MainLayoutProps> = ({ children }) => {
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
                {children}
            </Box>
        </Box>
    );
};

export default MainLayout;