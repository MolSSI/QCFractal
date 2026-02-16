import { List, ListItem, ListItemText } from "@mui/material";

const isEmpty = (value: any) =>
  value == null ||
  (typeof value === "object" && Object.keys(value).length === 0);

const Specification = ({ data }: { data: Record<string, any> }) => {
  const renderList = (obj: any): JSX.Element => {
    if (typeof obj !== "object" || obj === null) {
      return <>{String(obj)}</>;
    }

    return (
      <List dense sx={{ pl: 2 }}>
        {Object.entries(obj).map(([key, value]) => (
          <ListItem key={key} disablePadding>
            <ListItemText
              primary={
                <>
                  <strong>{key}:</strong>{" "}
                  {typeof value === "object" && !isEmpty(value) ? (
                    renderList(value)
                  ) : (
                    <>{isEmpty(value) ? "None" : String(value)}</>
                  )}
                </>
              }
            />
          </ListItem>
        ))}
      </List>
    );
  };

  return <>{renderList(data)}</>;
};

export default Specification;
