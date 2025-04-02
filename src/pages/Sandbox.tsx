import MainLayout from '../layouts/MainLayout';
import {usePortalClientAuth, usePortalClientRequest} from "../usePortalClient"
import {useState, useEffect} from "react";
import * as qcpTypes from "../PortalTypes";
import RecordStatus from "../components/RecordStatus";
import CalculationRecord from "../components/CalculationRecord";

function SandboxPage() {
    const {connectionState, login, logout} = usePortalClientAuth();
    const {makeRequest} = usePortalClientRequest();

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
        <MainLayout>

            <h1>Login/out</h1>
            <div>
                {connectionState.connected ? (
                    <>
                        <p>User: {connectionState.userInfo?.username ? connectionState.userInfo?.username : 'anonymous'}</p>
                        <button onClick={() => {
                            logout(false);
                        }}>Logout
                        </button>
                    </>
                ) : (
                    <>
                        <p>Not logged in</p>
                        <button onClick={() => login("guitest", "rJg_eghnr7QJF1r9bAkkEA")}>Login</button>
                    </>
                )}
            </div>

            <h1>Sandbox</h1>
            {<RecordStatus projectId={1} recordId={118868175} recordStatus={"waiting"}/>}
            {<CalculationRecord projectId={1} recordId={118868175} />}
        </MainLayout>
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
