'use client';

import { useLocale, useTranslations } from 'next-intl';
import { useId, useState, useTransition } from 'react';
import { Icon } from '@/components/brand/icon';
import { BotFields } from '@/components/site/forms/bot-fields';
import { Button } from '@/components/site/ui/button';
import { Checkbox, Chip, describedBy, Field, Input, Select, Textarea } from '@/components/site/ui/field';
import { sendPrivateDiningInquiry } from '@/lib/actions/commerce';
import { normalizeDigits } from '@/lib/i18n/digits';
import { plural } from '@/lib/i18n/plural';

type Kind = 'private_dining' | 'catering';

export interface InquiryHouse {
  slug: string;
  name: string;
  city: string;
  rooms: { slug: string; name: string; max: number }[];
}

export interface InquiryPackage {
  id: string;
  name: string;
  kind: Kind;
  minGuests: number;
}

interface InquiryFormProps {
  houses: InquiryHouse[];
  packages: InquiryPackage[];
  today: string;
  initial: { kind: Kind; room: string; package: string };
}

/** The private dining and catering request: what, where, when, how many, and how to reach you. */
export function InquiryForm({ houses, packages, today, initial }: InquiryFormProps) {
  const t = useTranslations('gather.privateDining.inquiry');
  const tf = useTranslations('forms');
  const tc = useTranslations('common');
  const locale = useLocale();
  const id = useId();
  const roomHouse = (slug: string) => houses.find((h) => h.rooms.some((r) => r.slug === slug));
  const [kind, setKind] = useState<Kind>(initial.kind);
  const [house, setHouse] = useState(roomHouse(initial.room)?.slug ?? '');
  const [room, setRoom] = useState(roomHouse(initial.room) ? initial.room : '');
  const [pkg, setPkg] = useState(packages.some((p) => p.id === initial.package && p.kind === initial.kind) ? initial.package : '');
  const [guests, setGuests] = useState('');
  const [errors, setErrors] = useState<Record<string, string>>({});
  const [formError, setFormError] = useState<string | null>(null);
  const [sent, setSent] = useState<string | null>(null);
  const [pending, start] = useTransition();

  const rooms = houses.flatMap((h) => h.rooms.map((r) => ({ ...r, house: h })));
  const chosenRoom = kind === 'private_dining' ? rooms.find((r) => r.slug === room) : undefined;
  const kindPackages = packages.filter((p) => p.kind === kind);
  const chosenPackage = kindPackages.find((p) => p.id === pkg);

  /** Guest count against the room's size and the menu's minimum; null when it fits. */
  const guestsProblem = (count: number): string | null => {
    if (chosenRoom && count > chosenRoom.max) return t('errors.roomSize', { room: chosenRoom.name, ...plural(chosenRoom.max, locale) });
    if (chosenPackage && count < chosenPackage.minGuests) return t('errors.minGuests', { package: chosenPackage.name, ...plural(chosenPackage.minGuests, locale) });
    return null;
  };
  const message = (field: string, code: string): string => {
    if (field === 'guests' && (code === 'roomSize' || code === 'minGuests')) return guestsProblem(Number(normalizeDigits(guests))) ?? tf('errors.invalid');
    return tf.has(`errors.${code}`) ? tf(`errors.${code}`) : tf('errors.invalid');
  };
  const err = (k: string) => (errors[k] ? message(k, errors[k]) : undefined);

  const chooseKind = (next: Kind) => {
    setKind(next);
    setPkg('');
    setRoom('');
    setErrors({});
  };

  if (sent) {
    return (
      <div role="status" className="flex flex-col gap-3 border-t border-ink pt-6">
        <p className="t-heading-md flex items-center gap-3">
          <Icon name="check" size={24} />
          {t('doneTitle')}
        </p>
        <p className="t-body measure">{t('done', { email: sent })}</p>
      </div>
    );
  }

  return (
    <form
      noValidate
      className="grid gap-6 md:grid-cols-2"
      onSubmit={(e) => {
        e.preventDefault();
        const form = new FormData(e.currentTarget);
        setFormError(null);
        const count = Number(normalizeDigits(guests));
        if (Number.isInteger(count) && count > 0 && guestsProblem(count)) {
          setErrors({ guests: chosenRoom && count > chosenRoom.max ? 'roomSize' : 'minGuests' });
          setFormError(tf('errors.validation'));
          return;
        }
        start(async () => {
          const res = await sendPrivateDiningInquiry(form);
          if (res.ok) setSent(res.data.email);
          else {
            setErrors(res.fieldErrors ?? {});
            setFormError(tf.has(`errors.${res.error}`) ? tf(`errors.${res.error}`) : tf('errors.unknown'));
          }
        });
      }}
    >
      <BotFields />
      <input type="hidden" name="locale" value={locale} />
      <input type="hidden" name="kind" value={kind} />
      <fieldset className="flex flex-col gap-3 md:col-span-2">
        <legend className="t-label mb-3 text-muted">{t('kind')}</legend>
        <div className="flex flex-wrap gap-2">
          {(['private_dining', 'catering'] as const).map((k) => (
            <Chip key={k} pressed={kind === k} onClick={() => chooseKind(k)}>
              {t(`kinds.${k}`)}
            </Chip>
          ))}
        </div>
      </fieldset>

      {kind === 'private_dining' ? (
        <>
          <Field id={`${id}-house`} label={t('house')}>
            <Select
              id={`${id}-house`}
              name="branch"
              value={house}
              onChange={(e) => {
                setHouse(e.target.value);
                if (room && roomHouse(room)?.slug !== e.target.value) setRoom('');
              }}
            >
              <option value="">{t('houseAny')}</option>
              {houses.map((h) => (
                <option key={h.slug} value={h.slug}>
                  {h.name}
                </option>
              ))}
            </Select>
          </Field>
          <Field id={`${id}-room`} label={t('room')} error={err('room')}>
            <Select
              id={`${id}-room`}
              name="room"
              value={room}
              onChange={(e) => {
                setRoom(e.target.value);
                const owner = roomHouse(e.target.value);
                if (owner) setHouse(owner.slug);
              }}
              {...describedBy(`${id}-room`, { error: Boolean(err('room')) })}
            >
              <option value="">{t('roomAny')}</option>
              {houses
                .filter((h) => !house || h.slug === house)
                .map((h) => (
                  <optgroup key={h.slug} label={h.name}>
                    {h.rooms.map((r) => (
                      <option key={r.slug} value={r.slug}>
                        {r.name}
                      </option>
                    ))}
                  </optgroup>
                ))}
            </Select>
          </Field>
        </>
      ) : (
        <Field id={`${id}-city`} label={t('cateringHouse')} className="md:col-span-2">
          <Select id={`${id}-city`} name="branch" defaultValue="">
            <option value="">{t('houseAny')}</option>
            {houses.map((h) => (
              <option key={h.slug} value={h.slug}>
                {h.city}
              </option>
            ))}
          </Select>
        </Field>
      )}

      <Field id={`${id}-date`} label={`${t('date')} (${tf('fields.optional')})`} hint={t('dateHint')} error={err('date')}>
        <Input id={`${id}-date`} name="date" type="date" min={today} {...describedBy(`${id}-date`, { hint: true, error: Boolean(err('date')) })} />
      </Field>
      <Field id={`${id}-guests`} label={t('guests')} required requiredLabel={tc('a11y.required')} error={err('guests')}>
        <Input
          id={`${id}-guests`}
          name="guests"
          inputMode="numeric"
          autoComplete="off"
          value={guests}
          onChange={(e) => {
            setGuests(e.target.value);
            if (errors.guests) setErrors(({ guests: _drop, ...rest }) => rest);
          }}
          required
          className="max-w-40 tabular"
          {...describedBy(`${id}-guests`, { error: Boolean(err('guests')) })}
        />
      </Field>
      <Field id={`${id}-package`} label={t('package')} error={err('package')} className="md:col-span-2">
        <Select id={`${id}-package`} name="package" value={pkg} onChange={(e) => setPkg(e.target.value)} {...describedBy(`${id}-package`, { error: Boolean(err('package')) })}>
          <option value="">{t('packageAny')}</option>
          {kindPackages.map((p) => (
            <option key={p.id} value={p.id}>
              {p.name}
            </option>
          ))}
        </Select>
      </Field>

      <Field id={`${id}-name`} label={tf('fields.name')} required requiredLabel={tc('a11y.required')} error={err('name')}>
        <Input id={`${id}-name`} name="name" autoComplete="name" required maxLength={80} {...describedBy(`${id}-name`, { error: Boolean(err('name')) })} />
      </Field>
      <Field id={`${id}-email`} label={tf('fields.email')} required requiredLabel={tc('a11y.required')} error={err('email')}>
        <Input id={`${id}-email`} name="email" type="email" autoComplete="email" required {...describedBy(`${id}-email`, { error: Boolean(err('email')) })} />
      </Field>
      <Field id={`${id}-phone`} label={tf('fields.phone')} hint={tf('fields.phoneHint')} required requiredLabel={tc('a11y.required')} error={err('phone')} className="md:col-span-2">
        <Input id={`${id}-phone`} name="phone" type="tel" autoComplete="tel" inputMode="tel" required className="md:max-w-80" {...describedBy(`${id}-phone`, { hint: true, error: Boolean(err('phone')) })} />
      </Field>
      <Field id={`${id}-message`} label={t('message')} hint={t('messageHint')} required requiredLabel={tc('a11y.required')} error={err('message')} className="md:col-span-2">
        <Textarea id={`${id}-message`} name="message" rows={6} required maxLength={3000} {...describedBy(`${id}-message`, { hint: true, error: Boolean(err('message')) })} />
      </Field>
      <div className="md:col-span-2">
        <Checkbox id={`${id}-consent`} name="consent" label={tf('consent.privacy')} required aria-invalid={Boolean(err('consent')) || undefined} />
        {err('consent') ? (
          <p role="alert" className="t-small mt-2 text-danger">
            {err('consent')}
          </p>
        ) : null}
      </div>
      <div className="flex flex-col gap-3 md:col-span-2">
        {formError ? (
          <p role="alert" className="t-small text-danger">
            {formError}
          </p>
        ) : null}
        <Button type="submit" size="lg" icon="arrow" disabled={pending} className="w-full sm:w-auto sm:self-start">
          {pending ? t('sending') : t('submit')}
        </Button>
      </div>
    </form>
  );
}
