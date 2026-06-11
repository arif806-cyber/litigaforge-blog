// src/content/config.ts
// Defines the schema for blog posts

import { defineCollection, z } from 'astro:content';

const blog = defineCollection({
  type: 'content',
  schema: z.object({
    title:           z.string(),
    description:     z.string(),
    date:            z.string(),
    country:         z.string(),
    legalArea:       z.string(),
    tags:            z.array(z.string()),
    readTime:        z.string(),
    author:          z.string().default('LitigaForge AI Editorial Team'),
    canonicalUrl:    z.string().optional(),
    schema:          z.string().optional(),
  }),
});

export const collections = { blog };
