import { defineConfig } from 'astro/config';
import tailwind from '@astrojs/tailwind';

export default defineConfig({
  site: 'https://litigaforge.com',
  integrations: [
    tailwind(),
  ],
  trailingSlash: 'never',
});
