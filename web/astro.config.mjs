import { defineConfig } from 'astro/config';
import react from '@astrojs/react';
import node from '@astrojs/node';
import AstroPWA from '@vite-pwa/astro';

export default defineConfig({
  output: 'server',
  adapter: node({ mode: 'standalone' }),
  integrations: [
    react(),
    AstroPWA({
      registerType: 'autoUpdate',
      workbox: {
        navigateFallback: '/',
        runtimeCaching: [
          {
            urlPattern: /^https:\/\/covers\.openlibrary\.org\/.*/i,
            handler: 'CacheFirst',
            options: {
              cacheName: 'covers',
              expiration: { maxEntries: 500, maxAgeSeconds: 30 * 24 * 60 * 60 },
            },
          },
          {
            urlPattern: /^https?:\/\/.*\/search\?.*/i,
            handler: 'NetworkFirst',
            options: {
              cacheName: 'search',
              expiration: { maxEntries: 50, maxAgeSeconds: 60 * 5 },
            },
          },
          {
            urlPattern: /^https?:\/\/.*\/(works|authors|editions)\/.*/i,
            handler: 'StaleWhileRevalidate',
            options: {
              cacheName: 'resources',
              expiration: { maxEntries: 200, maxAgeSeconds: 24 * 60 * 60 },
            },
          },
          {
            urlPattern: /^https?:\/\/.*\/auth\/.*/i,
            handler: 'NetworkOnly',
          },
        ],
      },
      manifest: false,
    }),
  ],
  devToolbar: { enabled: false },
  prefetch: {
    prefetchAll: true,
    defaultStrategy: 'hover',
  },
});
