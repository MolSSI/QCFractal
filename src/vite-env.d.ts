/// <reference types="vite/client" />

interface ImportMetaEnv {
  readonly VITE_QCFRACTAL_URI: string
}

interface ImportMeta {
  readonly env: ImportMetaEnv
}
