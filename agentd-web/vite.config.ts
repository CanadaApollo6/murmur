import { defineConfig } from "vitest/config";
import react from "@vitejs/plugin-react";

// The bundle has to satisfy the surface's CSP: `default-src 'none'` with only
// same-origin scripts, styles, images and fonts. Nothing may be inlined.
export default defineConfig({
  plugins: [react()],
  build: {
    assetsInlineLimit: 0,
    cssCodeSplit: false,
    modulePreload: { polyfill: false },
    sourcemap: false,
  },
  test: {
    environment: "node",
    include: ["src/**/*.test.ts"],
  },
});
