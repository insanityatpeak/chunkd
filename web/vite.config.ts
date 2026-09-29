import { defineConfig } from 'vite';
import preact from '@preact/preset-vite';

export default defineConfig({
  // Relative, so one build serves from GitHub Pages (/chunkd/) and from the
  // gateway (/).
  base: './',
  plugins: [preact()],
  worker: { format: 'es' },
  build: { target: 'es2022' },
});
