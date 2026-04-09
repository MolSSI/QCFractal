/// <reference types="vite/client" />

interface ImportMetaEnv {
  readonly VITE_QCFRACTAL_URI: string;
  readonly VITE_FEEDBACK_URL: string;
}

interface ImportMeta {
  readonly env: ImportMetaEnv;
}
