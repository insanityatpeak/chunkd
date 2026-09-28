import { defineConfig } from 'vite';
import preact from '@preact/preset-vite';

// base matches the GitHub Pages project path: https://insanityatpeak.github.io/chunkd/
export default defineConfig({
  base: '/chunkd/',
  plugins: [preact()],
  worker: { format: 'es' },
  build: { target: 'es2022' },
});
