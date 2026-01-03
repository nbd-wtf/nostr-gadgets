import { defineConfig } from 'vite'

export default defineConfig({
  server: {
    port: 7060,
  },
  build: {
    target: 'esnext',
  },
})
