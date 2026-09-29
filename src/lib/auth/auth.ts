import 'server-only';
import { betterAuth } from 'better-auth';
import { drizzleAdapter } from 'better-auth/adapters/drizzle';
import { nextCookies } from 'better-auth/next-js';
import { emailOTP } from 'better-auth/plugins/email-otp';
import { db } from '@/lib/db/client';
import { accounts, sessions, users, verifications } from '@/lib/db/schema';
import { mail } from '@/lib/server/mail';
import { siteUrl } from '@/lib/site';
import { createId } from '@/lib/utils/id';

type HeaderSource = { headers?: Headers | null; request?: Request | null } | null | undefined;

/** Best-effort locale for auth emails: explicit header, then the referring page's locale prefix. */
function localeFrom(ctx: HeaderSource): 'ar' | 'en' {
  const headers = ctx?.headers ?? ctx?.request?.headers ?? null;
  const explicit = headers?.get('x-zill-locale');
  if (explicit === 'en' || explicit === 'ar') return explicit;
  const referer = headers?.get('referer') ?? '';
  return /\/en(\/|$|\?)/.test(referer) ? 'en' : 'ar';
}

export const auth = betterAuth({
  appName: 'Zill',
  baseURL: process.env.BETTER_AUTH_URL ?? siteUrl(),
  secret: process.env.BETTER_AUTH_SECRET,
  trustedOrigins: [siteUrl()],
  database: drizzleAdapter(db, {
    provider: 'sqlite',
    schema: { user: users, session: sessions, account: accounts, verification: verifications },
  }),
  emailAndPassword: {
    enabled: true,
    minPasswordLength: 8,
    maxPasswordLength: 128,
    autoSignIn: true,
    resetPasswordTokenExpiresIn: 3600,
    sendResetPassword: async ({ user, url }, request) => {
      await mail(user.email, { name: 'reset-password', props: { locale: localeFrom({ request }), name: user.name, url } });
    },
  },
  user: {
    additionalFields: {
      role: { type: 'string', defaultValue: 'customer', input: false },
      phone: { type: 'string', required: false },
      locale: { type: 'string', required: false, defaultValue: 'ar' },
    },
  },
  session: {
    expiresIn: 60 * 60 * 24 * 30,
    updateAge: 60 * 60 * 24,
    cookieCache: { enabled: true, maxAge: 60 * 5 },
  },
  rateLimit: {
    enabled: true,
    window: 60,
    max: 60,
    customRules: {
      '/sign-in/email': { window: 60, max: 6 },
      '/sign-up/email': { window: 60, max: 5 },
      '/email-otp/send-verification-otp': { window: 60, max: 3 },
      '/sign-in/email-otp': { window: 60, max: 6 },
      '/request-password-reset': { window: 60, max: 3 },
    },
  },
  advanced: {
    cookiePrefix: 'zill',
    useSecureCookies: process.env.NODE_ENV === 'production' && siteUrl().startsWith('https'),
    database: { generateId: () => createId() },
  },
  plugins: [
    emailOTP({
      otpLength: 6,
      expiresIn: 600,
      allowedAttempts: 5,
      async sendVerificationOTP({ email, otp, type }, ctx) {
        await mail(email, { name: 'otp', props: { locale: localeFrom(ctx), code: otp, purpose: type } }, { meta: { otp, type } });
      },
    }),
    nextCookies(),
  ],
});

export type Auth = typeof auth;
