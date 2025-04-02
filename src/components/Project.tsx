// src/pages/Profile.tsx
import {usePortalClientRequest} from "../usePortalClient.ts";
import {useEffect, useState} from "react";
import * as qcpTypes from "../PortalTypes.ts";
import {useParams} from "react-router-dom";

export default function Project() {

    const { projectId } = useParams();

    const { connectionState, makeRequest } = usePortalClientRequest(); // Get client instance here
    const [data, setData] = useState<qcpTypes.Project | undefined>(undefined);
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState<string | undefined>(undefined);

    useEffect(() => {
        async function fetchData() {
            setLoading(true);
            const {data, error} = await makeRequest<qcpTypes.Project>(
                "get",
                `api/v1/projects/${projectId}`,
            );
            setData(data)
            setError(error)
            setLoading(false);
        }
        fetchData();
    }, [projectId, connectionState, makeRequest]);

    return (
        <>
            {loading && <p>Loading...</p>}
            {data && JSON.stringify(data, null, 2)}
            {error && <p>{error}</p>}
        </>
    );
}
