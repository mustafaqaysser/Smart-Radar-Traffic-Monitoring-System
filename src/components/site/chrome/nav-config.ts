import type { FeatureFlag } from '@/lib/config/define';

export interface NavItem {
  href: string;
  key: string;
  feature?: FeatureFlag;
}

export interface NavGroup {
  key: 'eat' | 'gather' | 'house' | 'visit';
  items: NavItem[];
}

/** Primary links shown in the header bar on wide screens. */
export const HEADER_LINKS: NavItem[] = [
  { href: '/menu', key: 'menu' },
  { href: '/experiences', key: 'experiences', feature: 'events' },
  { href: '/story', key: 'story' },
  { href: '/locations', key: 'locations' },
];

/** The full navigation (overlay and footer). */
export const NAV_GROUPS: NavGroup[] = [
  {
    key: 'eat',
    items: [
      { href: '/menu', key: 'menu' },
      { href: '/order', key: 'order', feature: 'ordering' },
      { href: '/gift-cards', key: 'giftCards', feature: 'giftCards' },
      { href: '/membership', key: 'membership', feature: 'loyalty' },
    ],
  },
  {
    key: 'gather',
    items: [
      { href: '/reserve', key: 'reserve', feature: 'reservations' },
      { href: '/experiences', key: 'experiences', feature: 'events' },
      { href: '/private-dining', key: 'privateDining', feature: 'privateDining' },
    ],
  },
  {
    key: 'house',
    items: [
      { href: '/story', key: 'story' },
      { href: '/team', key: 'team' },
      { href: '/gallery', key: 'gallery' },
      { href: '/journal', key: 'journal', feature: 'journal' },
      { href: '/press', key: 'press' },
      { href: '/reviews', key: 'reviews', feature: 'reviews' },
    ],
  },
  {
    key: 'visit',
    items: [
      { href: '/locations', key: 'locations' },
      { href: '/contact', key: 'contact' },
      { href: '/faq', key: 'faq' },
      { href: '/concierge', key: 'concierge', feature: 'aiConcierge' },
      { href: '/careers', key: 'careers', feature: 'careers' },
    ],
  },
];

export function visible(items: NavItem[], features: Partial<Record<FeatureFlag, boolean>>): NavItem[] {
  return items.filter((i) => !i.feature || features[i.feature] !== false);
}
