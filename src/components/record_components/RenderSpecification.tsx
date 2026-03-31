import React from "react";
import { GenericDataList, GenericDataListKey } from "../GenericDataList.tsx";

export interface RenderSpecificationProps {
  data: Record<string, unknown>;
  keys: GenericDataListKey[];
}

export const RenderSpecification: React.FC<RenderSpecificationProps> = ({ data, keys }) => {
  return (
    <GenericDataList data={data} keys={keys} />
  );
};
