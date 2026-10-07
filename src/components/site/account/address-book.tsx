'use client';

import { useLocale, useTranslations } from 'next-intl';
import { useId, useState, useTransition } from 'react';
import { Icon } from '@/components/brand/icon';
import { Button } from '@/components/site/ui/button';
import { Checkbox, describedBy, Field, Input } from '@/components/site/ui/field';
import { Dialog } from '@/components/site/ui/dialog';
import { toast } from '@/components/site/ui/toast';
import { useRouter } from '@/i18n/navigation';
import { deleteAddress, saveAddress } from '@/lib/actions/account';
import { joinParts } from '@/lib/i18n/format';

export interface SavedAddressView {
  id: string;
  label: string;
  area: string;
  street: string;
  building: string | null;
  floor: string | null;
  notes: string | null;
  isDefault: boolean;
}

/** Saved delivery addresses: list, add, edit inline, make default, remove with a confirmation. */
export function AddressBook({ addresses }: { addresses: SavedAddressView[] }) {
  const t = useTranslations('account.addresses');
  const locale = useLocale();
  const tc = useTranslations('common');
  const router = useRouter();
  const [editing, setEditing] = useState<string | 'new' | null>(addresses.length ? null : 'new');
  const [removing, setRemoving] = useState<SavedAddressView | null>(null);
  const [pending, start] = useTransition();
  const tf = useTranslations('forms');
  const failMessage = (error: string) => (tf.has(`errors.${error}`) ? tf(`errors.${error}`) : tf('errors.unknown'));

  return (
    <div className="flex flex-col gap-8">
      {addresses.length ? (
        <ul className="border-t border-ink">
          {addresses.map((a) =>
            editing === a.id ? (
              <li key={a.id} className="border-b border-line py-6">
                <AddressForm initial={a} onDone={() => setEditing(null)} />
              </li>
            ) : (
              <li key={a.id} className="grid gap-3 border-b border-line py-6 sm:grid-cols-[1fr_auto] sm:items-start">
                <div className="flex flex-col gap-1">
                  <p className="t-heading-sm flex flex-wrap items-center gap-3">
                    <Icon name="pin" size={18} />
                    <bdi>{a.label}</bdi>
                    {a.isDefault ? <span className="t-label text-accent">{t('default')}</span> : null}
                  </p>
                  <p className="t-body" dir="auto">
                    {joinParts([a.street, a.area], locale)}
                  </p>
                  {a.building || a.floor ? <p className="t-small text-muted">{[a.building ? `${t('building')}: ${a.building}` : null, a.floor ? `${t('floor')}: ${a.floor}` : null].filter(Boolean).join(' · ')}</p> : null}
                  {a.notes ? (
                    <p className="t-small text-muted" dir="auto">
                      {a.notes}
                    </p>
                  ) : null}
                </div>
                <div className="flex flex-wrap gap-x-5 gap-y-2 sm:justify-end">
                  {!a.isDefault ? (
                    <button
                      type="button"
                      disabled={pending}
                      className="t-small min-h-11 underline decoration-line underline-offset-4"
                      onClick={() =>
                        start(async () => {
                          const res = await saveAddress({ ...a, building: a.building ?? '', floor: a.floor ?? '', notes: a.notes ?? '', isDefault: true });
                          if (res.ok) router.refresh();
                          else toast(failMessage(res.error));
                        })
                      }
                    >
                      {t('makeDefault')}
                    </button>
                  ) : null}
                  <button type="button" className="t-small min-h-11 underline decoration-line underline-offset-4" onClick={() => setEditing(a.id)}>
                    {t('edit')}
                  </button>
                  <button type="button" className="t-small min-h-11 text-danger underline decoration-line underline-offset-4" onClick={() => setRemoving(a)}>
                    {t('remove')}
                  </button>
                </div>
              </li>
            ),
          )}
        </ul>
      ) : (
        <p className="t-body text-muted">{t('none')}</p>
      )}

      {editing === 'new' ? (
        <section className="flex flex-col gap-6 bg-raised p-6 md:p-8" aria-label={t('add')}>
          <h2 className="t-heading-md">{t('add')}</h2>
          <AddressForm initial={null} onDone={() => setEditing(null)} canCancel={addresses.length > 0} />
        </section>
      ) : (
        <Button variant="secondary" icon={null} leadingIcon="plus" className="self-start" onClick={() => setEditing('new')}>
          {t('add')}
        </Button>
      )}

      <Dialog
        open={removing !== null}
        onClose={() => setRemoving(null)}
        title={removing ? t('removeConfirm', { label: removing.label }) : ''}
        closeLabel={tc('a11y.close')}
        footer={
          <div className="flex flex-wrap justify-end gap-3">
            <Button variant="secondary" icon={null} onClick={() => setRemoving(null)}>
              {t('cancel')}
            </Button>
            <Button
              icon={null}
              disabled={pending}
              onClick={() =>
                start(async () => {
                  if (!removing) return;
                  const res = await deleteAddress(removing.id);
                  setRemoving(null);
                  if (res.ok) {
                    toast(t('removed'));
                    router.refresh();
                  } else toast(failMessage(res.error));
                })
              }
            >
              {t('remove')}
            </Button>
          </div>
        }
      >
        {removing ? <p className="t-body">{joinParts([removing.street, removing.area], locale)}</p> : null}
      </Dialog>
    </div>
  );
}

function AddressForm({ initial, onDone, canCancel = true }: { initial: SavedAddressView | null; onDone: () => void; canCancel?: boolean }) {
  const t = useTranslations('account.addresses');
  const tf = useTranslations('forms');
  const tc = useTranslations('common');
  const router = useRouter();
  const id = useId();
  const [errors, setErrors] = useState<Record<string, string>>({});
  const [formError, setFormError] = useState<string | null>(null);
  const [pending, start] = useTransition();
  const err = (k: string) => (errors[k] ? tf(`errors.${errors[k]}`) : undefined);
  return (
    <form
      noValidate
      className="grid gap-6 sm:grid-cols-2"
      onSubmit={(e) => {
        e.preventDefault();
        const data = new FormData(e.currentTarget);
        const value = (k: string) => String(data.get(k) ?? '');
        start(async () => {
          const res = await saveAddress({
            id: initial?.id,
            label: value('label'),
            area: value('area'),
            street: value('street'),
            building: value('building'),
            floor: value('floor'),
            notes: value('notes'),
            isDefault: data.get('isDefault') === 'on' || initial?.isDefault,
          });
          if (res.ok) {
            toast(t('saved'));
            onDone();
            router.refresh();
          } else {
            setErrors(res.fieldErrors ?? {});
            setFormError(tf.has(`errors.${res.error}`) ? tf(`errors.${res.error}`) : tf('errors.unknown'));
          }
        });
      }}
    >
      <Field id={`${id}-label`} label={t('label')} required requiredLabel={tc('a11y.required')} error={err('label')}>
        <Input id={`${id}-label`} name="label" maxLength={40} required defaultValue={initial?.label ?? ''} placeholder={t('labelPlaceholder')} {...describedBy(`${id}-label`, { error: Boolean(err('label')) })} />
      </Field>
      <Field id={`${id}-area`} label={t('area')} required requiredLabel={tc('a11y.required')} error={err('area')}>
        <Input id={`${id}-area`} name="area" autoComplete="address-level3" maxLength={80} required defaultValue={initial?.area ?? ''} {...describedBy(`${id}-area`, { error: Boolean(err('area')) })} />
      </Field>
      <Field id={`${id}-street`} label={t('street')} required requiredLabel={tc('a11y.required')} error={err('street')} className="sm:col-span-2">
        <Input id={`${id}-street`} name="street" autoComplete="street-address" maxLength={160} required defaultValue={initial?.street ?? ''} {...describedBy(`${id}-street`, { error: Boolean(err('street')) })} />
      </Field>
      <Field id={`${id}-building`} label={`${t('building')} (${tf('fields.optional')})`} error={err('building')}>
        <Input id={`${id}-building`} name="building" maxLength={40} defaultValue={initial?.building ?? ''} />
      </Field>
      <Field id={`${id}-floor`} label={`${t('floor')} (${tf('fields.optional')})`} error={err('floor')}>
        <Input id={`${id}-floor`} name="floor" maxLength={40} defaultValue={initial?.floor ?? ''} />
      </Field>
      <Field id={`${id}-notes`} label={`${t('notes')} (${tf('fields.optional')})`} hint={t('notesHint')} error={err('notes')} className="sm:col-span-2">
        <Input id={`${id}-notes`} name="notes" maxLength={240} defaultValue={initial?.notes ?? ''} {...describedBy(`${id}-notes`, { hint: true })} />
      </Field>
      {!initial?.isDefault ? <Checkbox id={`${id}-default`} name="isDefault" label={t('makeDefault')} className="sm:col-span-2" /> : null}
      {formError ? (
        <p role="alert" className="t-small text-danger sm:col-span-2">
          {formError}
        </p>
      ) : null}
      <div className="flex flex-wrap items-center gap-4 sm:col-span-2">
        <Button type="submit" icon={null} disabled={pending}>
          {pending ? t('saving') : t('save')}
        </Button>
        {canCancel ? (
          <button type="button" className="t-small min-h-11 underline decoration-line underline-offset-4" onClick={onDone}>
            {t('cancel')}
          </button>
        ) : null}
      </div>
    </form>
  );
}
