import type { Role } from '@/lib/db/schema';

/**
 * Demo-mode staff accounts (DEMO_MODE=true, the default). The seed creates them and the staff sign-in page lists
 * them so every role can be tried. With DEMO_MODE=false the seed creates the owner only, and the setup script
 * prints a freshly generated password for it.
 */
export const DEMO_PASSWORD = 'zill-demo-2026';

export const DEMO_STAFF: { role: Role; email: string; name: string; branch: string | null }[] = [
  { role: 'owner', email: 'owner@zill.test', name: 'Lamees Qutub', branch: null },
  { role: 'manager', email: 'manager@zill.test', name: 'Reem Bakhsh', branch: null },
  { role: 'host', email: 'host@zill.test', name: 'Dana Al-Ghamdi', branch: 'al-balad' },
  { role: 'kitchen', email: 'kitchen@zill.test', name: 'Majid Hawsawi', branch: 'al-balad' },
  { role: 'waiter', email: 'waiter@zill.test', name: 'Omar Fallatah', branch: 'al-balad' },
  { role: 'editor', email: 'editor@zill.test', name: 'Sara Al-Qahtani', branch: null },
];
