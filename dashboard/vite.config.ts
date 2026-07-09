import react from "@vitejs/plugin-react";
import { defineConfig } from "vite";

// Dev flow: `tempo-tb-ingest replay <recording> --listen 127.0.0.1:8080` (or the
// live daemon) in one terminal, `npm run dev` here; API calls are proxied.
const backend = process.env.INGEST_BACKEND ?? "http://127.0.0.1:8080";

export default defineConfig({
  plugins: [react()],
  server: {
    proxy: {
      "/state": backend,
      "/healthz": backend,
      "/events": { target: backend, ws: true },
    },
  },
  build: { outDir: "dist" },
  test: {
    environment: "node",
    include: ["src/**/*.test.ts"],
  },
});
