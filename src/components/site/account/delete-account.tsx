'use client';

import { useTranslations } from 'next-intl';
import { useId, useState, useTransition } from 'react';
import { Button } from '@/components/site/ui/button';
import { Dialog } from '@/components/site/ui/dialog';
import { describedBy, Field, Input } from '@/components/site/ui/field';
import { useRouter } from '@/i18n/navigation';
import { deleteMyAccount } from '@/lib/actions/account';

/** Account deletion behind a confirmation that asks for the account's email. */
export function DeleteAccount({ email, blocked, cancels }: { email: string; blocked: boolean; cancels: string | null }) {
  const t = useTranslations('account.privacy.delete');
  const tf = useTranslations('forms');
  const tc = useTranslations('common');
  const router = useRouter();
  const id = useId();
  const [open, setOpen] = useState(false);
  const [typed, setTyped] = useState('');
  const [error, setError] = useState<string | null>(null);
  const [pending, start] = useTransition();
  const matches = typed.trim().toLowerCase() === email.toLowerCase();

  return (
    <>
      <Button variant="secondary" icon={null} leadingIcon="trash" disabled={blocked} onClick={() => setOpen(true)} className="self-start border-danger text-danger">
        {t('open')}
      </Button>
      <Dialog
        open={open}
        onClose={() => setOpen(false)}
        title={t('dialogTitle')}
        closeLabel={tc('a11y.close')}
        footer={
          <div className="flex flex-wrap justify-end gap-3">
            <Button variant="secondary" icon={null} onClick={() => setOpen(false)}>
              {tc('actions.cancel')}
            </Button>
            <Button
              icon={null}
              disabled={!matches || pending}
              onClick={() =>
                start(async () => {
                  setError(null);
                  const res = await deleteMyAccount({ confirmEmail: typed });
                  if (res.ok) {
                    setOpen(false);
                    router.replace('/account/deleted');
                    router.refresh();
                  } else setError(res.fieldErrors?.confirmEmail ? tf(`errors.${res.fieldErrors.confirmEmail}`) : tf.has(`errors.${res.error}`) ? tf(`errors.${res.error}`) : tf('errors.unknown'));
                })
              }
              className="bg-danger"
            >
              {pending ? t('deleting') : t('cta')}
            </Button>
          </div>
        }
      >
        <div className="flex flex-col gap-5">
          <p className="t-body">{t('body')}</p>
          {cancels ? <p className="t-body">{cancels}</p> : null}
          <p className="t-small text-danger">{t('irreversible')}</p>
          <Field id={`${id}-confirm`} label={t('confirm')} error={error ?? undefined}>
            <Input id={`${id}-confirm`} type="email" autoComplete="off" value={typed} onChange={(e) => setTyped(e.target.value)} placeholder={email} {...describedBy(`${id}-confirm`, { error: Boolean(error) })} />
          </Field>
        </div>
      </Dialog>
    </>
  );
}
