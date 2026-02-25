import { createRoot } from "react-dom/client";
import App from "./App.tsx";

createRoot(document.getElementById("root")!).render(<App />);

const loader = document.getElementById("initial-loader");
if (loader) {
  loader.remove();
}