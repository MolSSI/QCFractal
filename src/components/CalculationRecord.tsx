import {useEffect, useState} from "react"
import {Box, Chip, Grid, Container, Typography} from '@mui/material';
import * as qcpTypes from "../PortalTypes.ts"
import {usePortalClientRequest} from "../usePortalClient";
import {useParams} from "react-router-dom";

export default function CalculationRecord() {

    const { projectId, recordId } = useParams();

    const { connectionState, makeRequest } = usePortalClientRequest(); // Get client instance here

    const [data, setData] = useState<qcpTypes.CalculationRecord | undefined>(undefined);
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState<string | undefined>(undefined);

    useEffect(() => {
        async function fetchData() {
            setLoading(true);
            const {data, error} = await makeRequest<qcpTypes.CalculationRecord>(
                "get",
                `api/v1/projects/${projectId}/records/${recordId}`,
            );
            setData(data)
            setError(error)
            setLoading(false);
        }
        fetchData();
    }, [projectId, recordId, connectionState, makeRequest]);

    return (
        <>
        {loading && <p>Loading...</p>}
        {data &&
            <>
            <Chip variant="outlined" label={data.record_type} />
            <Typography>Record {recordId}</Typography>
            <Typography>Record {recordId}</Typography>
            <Typography>Record {recordId}</Typography>
            <Typography>Something</Typography>
            <Chip variant="outlined" label={data.status} />
            </>
        }
        </>
    );
}

/*
<Box sx={{ flexGrow: 1 }}>
    <Grid container spacing={1}>
        <Grid size={8}>
            <Container maxWidth="lg">
                <Chip variant="outlined" label={data.record_type} />
                <Typography>Record {recordId}</Typography>
                <Typography>Record {recordId}</Typography>
                <Typography>Record {recordId}</Typography>
            </Container>
        </Grid>
        <Grid size={6}>
            <Typography>Something</Typography>
        </Grid>
    </Grid>
*/