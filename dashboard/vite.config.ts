import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

export default defineConfig({
  plugins: [react()],
  build: { outDir: "dist", sourcemap: true },
  server: {
    port: 5173,
    // In development the dashboard talks to a node directly. In the Compose stack nginx
    // serves these files and proxies /api to node 0, so the same relative paths work in
    // both places and the app never needs to know a node's address.
    proxy: {
      "/api": {
        target: process.env.LAUNCHOS_NODE ?? "http://127.0.0.1:8080",
        changeOrigin: true,
        rewrite: (path) => path.replace(/^\/api/, ""),
      },
    },
  },
});
