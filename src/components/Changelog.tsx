import React from "react";
import {
  Accordion,
  AccordionDetails,
  AccordionSummary,
  Box,
  Divider,
  Typography,
} from "@mui/material";
import ExpandMoreIcon from "@mui/icons-material/ExpandMore";

type ChangeKind = "Added" | "Improved" | "Fixed";

interface ChangelogItem {
  kind: ChangeKind;
  text: string;
}

interface ChangelogEntry {
  /** Date of the changes, formatted as YYYY-MM-DD. */
  date: string;
  items: ChangelogItem[];
}

// Number of most-recent dates shown by default. Older dates are collapsed
// into the "Previous Updates" accordion.
const VISIBLE_DATE_COUNT = 3;

// User-facing changes, newest first. To record a change, add an item to the
// matching date (or a new dated entry at the top). Keep entries user-facing;
// skip purely internal refactors.
const CHANGELOG: ChangelogEntry[] = [
  {
    date: "2026-09-15",
    items: [
      { kind: "Added", text: "Edit dataset name and metadata" },
      {
        kind: "Added",
        text: "Edit specification names and delete specifications (along with their attached records)",
      },
      {
        kind: "Improved",
        text: "Paginated dataset status view and preserved dataset view state",
      },
      {
        kind: "Improved",
        text: "Spinning animation on the dataset page refresh button",
      },
      {
        kind: "Fixed",
        text: "Default tag and priority no longer display as empty",
      },
    ],
  },
  {
    date: "2026-09-11",
    items: [
      {
        kind: "Improved",
        text: "Server-side limit on the number of claimed records shown on the manager page",
      },
    ],
  },
  {
    date: "2026-09-08",
    items: [{ kind: "Added", text: "Refresh button on the manager page" }],
  },
  {
    date: "2026-09-01",
    items: [
      { kind: "Added", text: "Create new users from the user management page" },
      {
        kind: "Added",
        text: "Admins can create groups and assign new users to them",
      },
    ],
  },
  {
    date: "2026-07-28",
    items: [
      {
        kind: "Added",
        text: '"Go to manager page" button in the manager dialog',
      },
    ],
  },
  {
    date: "2026-07-16",
    items: [
      {
        kind: "Fixed",
        text: "Estimated CPU hours in server statistics (plus a fun-units display)",
      },
    ],
  },
  {
    date: "2026-07-06",
    items: [
      { kind: "Added", text: "Filter users by active or disabled status" },
    ],
  },
  {
    date: "2026-07-01",
    items: [
      {
        kind: "Added",
        text: "Group management — group column and management panel in the user list, plus batch group assignment",
      },
      {
        kind: "Added",
        text: "Admins can reset user passwords; admin tools moved to a dedicated sidebar section",
      },
    ],
  },
  {
    date: "2026-06-17",
    items: [{ kind: "Fixed", text: "Output dialog scrolling issues" }],
  },
  {
    date: "2026-06-15",
    items: [
      {
        kind: "Added",
        text: "Task list table on the manager page, filterable by status",
      },
    ],
  },
  {
    date: "2026-06-12",
    items: [
      {
        kind: "Added",
        text: "Direct link from record relationships to a record in a dataset",
      },
    ],
  },
  {
    date: "2026-06-11",
    items: [{ kind: "Fixed", text: "Invisible pie chart on Safari" }],
  },
  {
    date: "2026-06-07",
    items: [{ kind: "Added", text: "API Access Page dialog" }],
  },
  {
    date: "2026-06-03",
    items: [
      { kind: "Added", text: "Dataset relationship button and dialog" },
      {
        kind: "Improved",
        text: "Parent projects are now displayed in the record relationship dialog",
      },
    ],
  },
  {
    date: "2026-06-02",
    items: [{ kind: "Added", text: "Server statistics page" }],
  },
  {
    date: "2026-05-20",
    items: [
      {
        kind: "Improved",
        text: "Enhanced performance for large output files with virtual scrolling and debouncing",
      },
    ],
  },
  {
    date: "2026-04-29",
    items: [
      { kind: "Added", text: "User management list for administrators" },
      {
        kind: "Improved",
        text: "Administrator profile now includes extra management options",
      },
    ],
  },
  {
    date: "2026-04-28",
    items: [
      { kind: "Added", text: "Lookup by project or dataset id/name" },
      { kind: "Added", text: "Record relationship dialog" },
    ],
  },
  {
    date: "2026-04-27",
    items: [{ kind: "Added", text: "User info page (and user modification)" }],
  },
  {
    date: "2026-04-13",
    items: [
      { kind: "Added", text: "Favoriting records" },
      { kind: "Added", text: "Dataset and project attachments" },
      { kind: "Added", text: "Project creation" },
      {
        kind: "Added",
        text: "Linking/Unlinking existing datasets to a project",
      },
      {
        kind: "Improved",
        text: "Enhanced record/dataset/project description display with Markdown support",
      },
      { kind: "Added", text: "Torsiondrive plots" },
      {
        kind: "Improved",
        text: "Manager page & fragment (including claimed records)",
      },
      { kind: "Improved", text: "Remove ANSI escape codes from raw output" },
    ],
  },
  {
    date: "2026-04-10",
    items: [
      {
        kind: "Improved",
        text: "Molecular formula formatting & molecule viewer layouts",
      },
      { kind: "Improved", text: "Remove ANSI escape codes from raw output" },
    ],
  },
  {
    date: "2026-04-09",
    items: [
      { kind: "Added", text: "This homepage" },
      { kind: "Added", text: "Dataset records and various record pages" },
    ],
  },
];

const ChangelogEntryBlock: React.FC<{ entry: ChangelogEntry }> = ({ entry }) => (
  <li style={{ marginTop: "16px" }}>
    <Typography variant="subtitle1" sx={{ fontWeight: "bold" }}>
      {entry.date}
    </Typography>
    <ul>
      {entry.items.map((item, i) => (
        <li key={i}>
          <strong>{item.kind}:</strong> {item.text}
        </li>
      ))}
    </ul>
  </li>
);

const Changelog: React.FC = () => {
  const visibleEntries = CHANGELOG.slice(0, VISIBLE_DATE_COUNT);
  const previousEntries = CHANGELOG.slice(VISIBLE_DATE_COUNT);
  const lastUpdated = CHANGELOG[0]?.date;

  // Collapsed by default so the changelog stays out of the way on the home
  // page; the header still surfaces the last-updated date.
  return (
    <Accordion sx={{ "&:before": { display: "none" } }}>
      <AccordionSummary expandIcon={<ExpandMoreIcon />} sx={{ px: 3 }}>
        <Box
          sx={{
            display: "flex",
            alignItems: "baseline",
            flexWrap: "wrap",
            columnGap: 1.5,
          }}
        >
          <Typography variant="h5">What's New / Changelog</Typography>
          {lastUpdated && (
            <Typography variant="body2" color="text.secondary">
              Last updated: {lastUpdated}
            </Typography>
          )}
        </Box>
      </AccordionSummary>
      <AccordionDetails sx={{ px: 3 }}>
        <Divider sx={{ mb: 2 }} />
        <Typography variant="body1" component="div">
          <ul style={{ listStyleType: "none", paddingLeft: 0 }}>
            {visibleEntries.map((entry) => (
              <ChangelogEntryBlock key={entry.date} entry={entry} />
            ))}

            {previousEntries.length > 0 && (
              <Accordion
                variant="outlined"
                sx={{ mt: 2, "&:before": { display: "none" } }}
              >
                <AccordionSummary expandIcon={<ExpandMoreIcon />}>
                  <Typography sx={{ fontWeight: "bold" }}>
                    Previous Updates
                  </Typography>
                </AccordionSummary>
                <AccordionDetails sx={{ pt: 0 }}>
                  <ul style={{ listStyleType: "none", paddingLeft: 0 }}>
                    {previousEntries.map((entry) => (
                      <ChangelogEntryBlock key={entry.date} entry={entry} />
                    ))}
                  </ul>
                </AccordionDetails>
              </Accordion>
            )}
          </ul>
        </Typography>
      </AccordionDetails>
    </Accordion>
  );
};

export default Changelog;
