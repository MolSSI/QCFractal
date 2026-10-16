/// <reference types="vite/client" />

declare const __QCFRACTAL_DEV_PROXY_TARGET__: string;

interface ImportMetaEnv {
  readonly VITE_QCFRACTAL_URI?: string;
}

interface ImportMeta {
  readonly env: ImportMetaEnv;
}
