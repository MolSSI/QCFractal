import React from "react";
import JsonViewer from "../JsonViewer.tsx";

const KEY_ORDER = [
  "program",
  "driver",
  "method",
  "basis",
  "protocols",
  "keywords",
];

const isPlainObject = (v: unknown): v is Record<string, unknown> =>
  v !== null && typeof v === "object" && !Array.isArray(v);

const orderSpec = (spec: Record<string, unknown>): Record<string, unknown> => {
  const rank = (k: string) => {
    const i = KEY_ORDER.indexOf(k);
    return i === -1 ? KEY_ORDER.length : i;
  };
  return Object.fromEntries(
    Object.entries(spec)
      .sort(([a], [b]) => rank(a) - rank(b))
      .map(([k, v]) => [
        k,
        k.endsWith("_specification") && isPlainObject(v) ? orderSpec(v) : v,
      ]),
  );
};

export interface RenderSpecificationProps {
  data: object;
  keys: string[];
  hideIfEmpty?: string[];
}

export const RenderSpecification: React.FC<RenderSpecificationProps> = ({
  data,
  keys,
  hideIfEmpty = [],
}) => {
  const values = data as Record<string, unknown>;
  const shown = Object.fromEntries(
    keys
      .filter((k) => !(hideIfEmpty.includes(k) && values[k] == null))
      .map((k) => [
        k,
        isPlainObject(values[k]) && k.endsWith("_specification")
          ? orderSpec(values[k])
          : (values[k] ?? null),
      ]),
  );

  return <JsonViewer value={shown} expandDepth={4} />;
};
