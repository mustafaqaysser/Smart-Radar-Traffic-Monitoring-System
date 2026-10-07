import { notFound } from 'next/navigation';

/** Any unknown path under a language renders the localised 404 inside the site's chrome. */
export default function CatchAll() {
  notFound();
}
