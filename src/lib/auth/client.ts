'use client';
import { createAuthClient } from 'better-auth/react';
import { emailOTPClient, inferAdditionalFields } from 'better-auth/client/plugins';
import type { Auth } from './auth';

export const authClient = createAuthClient({
  plugins: [emailOTPClient(), inferAdditionalFields<Auth>()],
  fetchOptions: {
    headers: typeof document !== 'undefined' ? { 'x-zill-locale': document.documentElement.lang || 'ar' } : undefined,
  },
});
