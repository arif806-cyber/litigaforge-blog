import { defineConfig } from 'astro/config';
import sitemap from '@astrojs/sitemap';
import tailwind from '@astrojs/tailwind';

export default defineConfig({
  site: 'https://litigaforge.com',
  integrations: [
    sitemap(),   // auto-generates sitemap.xml from all pages
    tailwind(),
  ],
  // Blog lives at litigaforge.com/blog/
  base: '/',
});
