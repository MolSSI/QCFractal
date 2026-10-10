import {
  Accordion,
  AccordionDetails,
  AccordionSummary,
  Paper,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  Typography,
} from "@mui/material";
import ExpandMoreIcon from "@mui/icons-material/ExpandMore";
import * as qcpTypes from "../PortalTypes";
import { dateStringToLocalTime } from "../Utils.ts";

interface CommentsProps {
  comments?: qcpTypes.RecordComment[];
}

export default function Comments({ comments = [] }: CommentsProps) {
  const nComments = comments.length;

  return (
    <Accordion>
      {/* Accordion Header */}
      <AccordionSummary
        expandIcon={<ExpandMoreIcon />}
        aria-controls="comments-content"
        id="comments-header"
      >
        <Typography variant="h6" fontWeight="bold">
          Comments ({nComments})
        </Typography>
      </AccordionSummary>

      {/* Accordion Content */}
      <AccordionDetails>
        <TableContainer component={Paper}>
          <Table size="small" aria-label="comments table">
            <TableHead>
              <TableRow>
                <TableCell>
                  <strong>Time/Date</strong>
                </TableCell>
                <TableCell>
                  <strong>User</strong>
                </TableCell>
                <TableCell>
                  <strong>Comment</strong>
                </TableCell>
              </TableRow>
            </TableHead>
            <TableBody>
              {nComments > 0 ? (
                comments.map((comment) => (
                  <TableRow key={comment.id}>
                    <TableCell>
                      {dateStringToLocalTime(comment.timestamp)}
                    </TableCell>
                    <TableCell>{comment.username}</TableCell>
                    <TableCell>{comment.comment}</TableCell>
                  </TableRow>
                ))
              ) : (
                <TableRow>
                  <TableCell colSpan={3} align="center">
                    No comments available.
                  </TableCell>
                </TableRow>
              )}
            </TableBody>
          </Table>
        </TableContainer>
      </AccordionDetails>
    </Accordion>
  );
}
