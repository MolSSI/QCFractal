import {useCallback, useEffect, useState} from "react"
import {Button, Dialog, DialogContent, DialogTitle} from '@mui/material';
import {usePortalClientRequest} from "../usePortalClient";
import * as qcpTypes from "../PortalTypes"

interface RecordStatusProps {
    projectId: number;
    recordId: number;
    recordStatus?: string;
}

export default function RecordStatus({ projectId, recordId, recordStatus }: RecordStatusProps) {

    const { connectionState, makeRequest } = usePortalClientRequest(); // Get client instance here

    const [data, setData] = useState<string | null>(null);
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState<string | null>(null);

    const getDialogText = useCallback(
        async () => {
        if (recordStatus == undefined) {
            return "???";
        }
        if (recordStatus === "waiting") {
            return JSON.stringify(await makeRequest<qcpTypes.WaitingReason>(
                "get", `api/v1/records/${recordId}/waiting_reason`
            ))
        }
        else if (recordStatus === "error") {
            return JSON.stringify(await makeRequest<qcpTypes.WaitingReason>(
                "get", `api/v1/records/${recordId}/waiting_reason`
            ))
        } else {
            return "(unknown)"
        }
    }, [makeRequest, recordStatus, recordId]);

    // For the dialog
    const [open, setOpen] = useState(false);

    useEffect(() => {
        async function fetchData() {
            setLoading(true);
            const txt = await getDialogText();
            setData(txt)
            setLoading(false);
        }
        fetchData();
    }, [getDialogText, connectionState]);

    const handleOpen = () => {
        recordStatus = undefined;
        setOpen(true);
    }
    const handleClose = () => setOpen(false);

    const renderMessage = () => {
        switch (recordStatus) {
            case "waiting":
                return <Button onClick={handleOpen}>waiting</Button>;
            case "error":
                return <Button onClick={handleOpen}>error</Button>;
            default:
                return <span>{recordStatus}</span>;
        }
    };

    return (
        <div>
            {renderMessage()}

            {/* Dialog for error, waiting reason, etc */}
            <Dialog open={open} onClose={handleClose}>
                <DialogTitle>Status Details</DialogTitle>
                <DialogContent>
                    { data }
                </DialogContent>
            </Dialog>
        </div>
    );
}