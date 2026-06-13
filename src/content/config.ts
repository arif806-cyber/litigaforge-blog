// src/content/config.ts
// Defines the schema for blog posts

import { defineCollection, z } from 'astro:content';

const blog = defineCollection({
  type: 'content',
  schema: z.object({
    title:           z.string(),
    description:     z.string(),
    date:            z.string(),
    dateModified:    z.string().optional(),
    country:         z.string(),
    legalArea:       z.string(),
    category:        z.string().optional(),
    tags:            z.array(z.string()),
    readTime:        z.string(),
    wordCount:       z.number().optional(),
    normalizedTopic: z.string().optional(),
    author:          z.string().default('LitigaForge AI Editorial Team'),
    authorUrl:       z.string().optional(),
    canonicalUrl:    z.string().optional(),
    schema:          z.string().optional(),
    faq:             z.array(z.object({ q: z.string(), a: z.string() })).default([]),
  }),
});

export const collections = { blog };
