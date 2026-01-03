import { defineConfig } from 'vitest/config'
import { preview } from '@vitest/browser-preview'

export default defineConfig({
  test: {
    browser: {
      provider: preview(),
      enabled: true,
      instances: [
        { browser: 'chromium' },
      ],
    },
  }
})
