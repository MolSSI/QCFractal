import {useCallback, useEffect, useState} from "react"
import {Button, Dialog, DialogContent, DialogTitle} from '@mui/material';
import * as qcpTypes from "../PortalTypes.ts"
import {usePortalClientRequest} from "../usePortalClient";

interface CalculatedRecordProps {
    projectId: number;
    recordId: number;
}

export default function CalculationRecord({ projectId, recordId }: CalculatedRecordProps) {

    const { connectionState, makeRequest } = usePortalClientRequest(); // Get client instance here

    const [data, setData] = useState<qcpTypes.CalculationRecord | undefined>(undefined);
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState<string | undefined>(undefined);

    useEffect(() => {
        async function fetchData() {
            setLoading(true);
            const {data, error} = await makeRequest<qcpTypes.CalculationRecord>(
                "get",
                `api/v1/records/${recordId}`,
            );
            setData(data)
            setError(error)
            setLoading(false);
        }
        fetchData();
    }, [recordId, connectionState, makeRequest]);

    return (
        <>
            {JSON.stringify(data, null, 2)}
            {error}
        </>
    );
}