import { notFound } from 'next/navigation';

/** Any unknown back-office address renders the admin's own not-found screen inside the shell. */
export default function UnknownAdminPage() {
  notFound();
}
