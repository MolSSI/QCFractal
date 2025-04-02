import {usePortalClientAuth, usePortalClientRequest} from "../usePortalClient.ts"
import {useEffect, useState} from "react";
import * as qcpTypes from "../PortalTypes.ts";
import RecordStatus from "./RecordStatus.tsx";
import CalculationRecord from "./CalculationRecord.tsx";
import {useNavigate} from "react-router-dom";

function SandboxPage() {
    const {connectionState, login, logout} = usePortalClientAuth();
    const {makeRequest} = usePortalClientRequest();

    const navigate = useNavigate();

    const [data, setData] = useState<string | null>(null);
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState<string | null>(null);

    useEffect(() => {
        async function fetchData() {
            setLoading(true);
            const {data: rdata, error} = await makeRequest<qcpTypes.ServerInfo>("GET", "api/v1/information");
            setData(JSON.stringify(rdata, null, 2));
            setError(error);
            setLoading(false);
        }

        fetchData();
    }, [makeRequest, connectionState]);

    return (
        <>
            <h1>Sandbox</h1>
            {<RecordStatus projectId={1} recordId={118868175} recordStatus={"waiting"}/>}
            {<CalculationRecord projectId={1} recordId={118868175} />}
        </>
    );
}

//{<pre>Connection Data: {JSON.stringify(connectionState, null, 2)}</pre>}
//{loading && <p>Loading ...</p>}
//{error && <p style={{ color: "red" }}>{error}</p>}
//{data && <pre>{data}</pre>}

export default SandboxPage;
//{<pre>Connection Data: {JSON.stringify(connectionState, null, 2)}</pre>}
//{loading && <p>Loading server info...</p>}
//{error && <p style={{ color: "red" }}>{error}</p>}
//{sandboxData && <pre>{JSON.stringify(sandboxData, null, 2)}</pre>}
