import React from "react";
import { Stack, Typography } from "@mui/material";
import { PieChart } from "@mui/x-charts/PieChart";
import * as qcpTypes from "../PortalTypes";

interface ManagerPieChartProps {
  managerData: qcpTypes.Manager;
}

export const ManagerPieChart: React.FC<ManagerPieChartProps> = ({
  managerData,
}) => {
  const returned =
    managerData.claimed -
    (managerData.active_tasks + managerData.successes + managerData.failures);

  return (
    <Stack alignItems="center" border={1} gap={2} p={1}>
      <Typography variant="h6">
        Tasks ({managerData.claimed} total claimed)
      </Typography>
      {managerData.claimed === 0 ? (
        <Typography variant="body1">(no claimed tasks)</Typography>
      ) : (
        <PieChart
          height={200}
          sx={{ width: "100%" }}
          colors={["blue", "green", "red", "orange"]}
          series={[
            {
              data: [
                {
                  id: 0,
                  value: managerData.active_tasks,
                  label: `${managerData.active_tasks} active`,
                },
                {
                  id: 1,
                  value: managerData.successes,
                  label: `${managerData.successes} success`,
                },
                {
                  id: 2,
                  value: managerData.failures,
                  label: `${managerData.failures} failed`,
                },
                {
                  id: 4,
                  value: returned,
                  label: `${returned} returned`,
                },
              ],
            },
          ]}
        />
      )}
    </Stack>
  );
};

export default ManagerPieChart;
