import MainLayout from '../layouts/MainLayout';
import {usePortalClient, usePortalClientFetch} from "../PortalClientContext.tsx"
import {useCallback} from "react";

function SandboxPage() {
    const { connectionState, client } = usePortalClient(); // Get client instance here

    const f = () => { return client.getProjectsList() }
    const { data: sandboxData, loading, error } = usePortalClientFetch(f, connectionState);


    return (
        <MainLayout>
            <h1>Sandbox</h1>
            {<pre>Connection Data: {JSON.stringify(connectionState, null, 2)}</pre>}
            {loading && <p>Loading server info...</p>}
            {error && <p style={{ color: "red" }}>{error}</p>}
            {sandboxData && <pre>{JSON.stringify(sandboxData, null, 2)}</pre>}
        </MainLayout>
    );
}

export default SandboxPage;