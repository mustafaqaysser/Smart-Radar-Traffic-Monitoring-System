'use client';

import { useLocale, useTranslations } from 'next-intl';
import { useId, useState } from 'react';
import Image from 'next/image';
import { Dialog } from '@/components/site/ui/dialog';
import { Button } from '@/components/site/ui/button';
import { Label, Textarea } from '@/components/site/ui/field';
import { Quantity } from '@/components/site/ui/quantity';
import { toast } from '@/components/site/ui/toast';
import { cart, useCart } from '@/lib/cart/store';
import { lineUnitPrice, validateModifiers } from '@/lib/domain/pricing';
import { formatMoney } from '@/lib/i18n/format';
import { plural } from '@/lib/i18n/plural';
import type { ItemView } from '@/lib/menu/view';
import { cn } from '@/lib/utils/cn';

interface QuickAddProps {
  item: ItemView | null;
  branchSlug: string;
  branchName: string;
  onClose: () => void;
  /** Add somewhere other than the site basket (the table's own order); the default adds to the basket. */
  onAdd?: (line: { slug: string; qty: number; optionIds: string[]; note: string }) => void;
}

function defaults(item: ItemView): string[] {
  return item.modifierGroups.flatMap((g) => g.options.filter((o) => o.isDefault && o.isAvailable).map((o) => o.id));
}

/** Choose modifiers, a note and a quantity, then add the dish to the order. Prices recalculate as you choose. */
export function QuickAdd({ item, branchSlug, branchName, onClose, onAdd }: QuickAddProps) {
  const t = useTranslations('menu.quickAdd');
  const tc = useTranslations('common');
  const tm = useTranslations('menu.item');
  const locale = useLocale();
  const id = useId();
  const current = useCart();
  const [key, setKey] = useState<string | null>(null);
  const [selected, setSelected] = useState<string[]>([]);
  const [note, setNote] = useState('');
  const [qty, setQty] = useState(1);
  const [error, setError] = useState<string | null>(null);

  // Reset the form whenever a different dish is opened (adjusting state during render).
  if (item && key !== item.slug) {
    setKey(item.slug);
    setSelected(defaults(item));
    setNote('');
    setQty(1);
    setError(null);
  }

  if (!item) return null;

  const groups = item.modifierGroups;
  const unit = lineUnitPrice({ unitPrice: item.price, optionIds: selected, groups });
  const otherBranch = !onAdd && current.lines.length > 0 && current.branchSlug !== null && current.branchSlug !== branchSlug;

  const toggle = (groupId: string, optionId: string, single: boolean) => {
    setError(null);
    setSelected((prev) => {
      const group = groups.find((g) => g.id === groupId);
      const inGroup = new Set(group?.options.map((o) => o.id));
      if (single) return [...prev.filter((x) => !inGroup.has(x)), optionId];
      if (prev.includes(optionId)) return prev.filter((x) => x !== optionId);
      const count = prev.filter((x) => inGroup.has(x)).length;
      if (group && count >= group.maxSelect) return prev;
      return [...prev, optionId];
    });
  };

  const submit = () => {
    const errors = validateModifiers(groups, selected);
    const missing = errors.find((e) => e.code === 'required');
    if (missing && missing.code === 'required') {
      setError(t('missing', { group: groups.find((g) => g.id === missing.groupId)?.name ?? '' }));
      return;
    }
    if (errors.length) {
      setError(t('unavailableOption'));
      return;
    }
    if (onAdd) {
      onAdd({ slug: item.slug, qty, optionIds: selected, note: note.trim() });
      toast(`${item.name} — ${tm('added')}`);
      onClose();
      return;
    }
    if (otherBranch) cart.replace([], branchSlug, current.channel);
    else cart.setBranch(branchSlug);
    cart.add(item.slug, qty, selected, note);
    toast(`${item.name} — ${tm('added')}`, { label: t('viewOrder'), href: '/order/cart' });
    onClose();
  };

  return (
    <Dialog
      open={Boolean(item)}
      onClose={onClose}
      title={item.name}
      closeLabel={tc('a11y.close')}
      variant="sheet"
      footer={
        <div className="flex flex-wrap items-center justify-between gap-4">
          <Quantity value={qty} onChange={setQty} label={t('quantity')} />
          <Button onClick={submit} icon={null} className="flex-1">
            {t('addFor', { price: formatMoney(unit * qty, locale) })}
          </Button>
        </div>
      }
    >
      <div className="flex flex-col gap-8">
        {item.image ? (
          <div className="arch-4x5 relative mx-auto aspect-[4/5] w-2/3 max-w-64 overflow-hidden bg-surface">
            <Image src={item.image.src} alt={item.image.alt} fill sizes="256px" quality={70} placeholder={item.image.blur ? 'blur' : 'empty'} blurDataURL={item.image.blur ?? undefined} className="object-cover" style={{ objectPosition: `${item.image.focalX * 100}% ${item.image.focalY * 100}%` }} />
          </div>
        ) : null}
        <p className="t-body text-muted">{item.description}</p>
        <p className="t-small text-muted">{t('branchNote', { house: branchName })}</p>
        {otherBranch ? <p className="t-small border-s-2 border-warning ps-3 text-warning">{t('switchBranch', { house: branchName })}</p> : null}

        {groups.map((g) => {
          const single = g.maxSelect === 1 && g.minSelect === 1;
          const hint = g.minSelect > 0 ? (single ? t('chooseOne') : t('required', plural(g.minSelect, locale))) : t('optional', plural(g.maxSelect, locale));
          return (
            <fieldset key={g.id} className="flex flex-col gap-3">
              <legend className="mb-3 flex w-full items-baseline justify-between gap-4">
                <span className="t-heading-sm">{g.name}</span>
                <span className="t-small text-muted">{hint}</span>
              </legend>
              {g.options.map((o) => {
                const checked = selected.includes(o.id);
                return (
                  <label key={o.id} className={cn('flex min-h-12 cursor-pointer items-center gap-3 border border-line px-4', checked && 'border-ink', !o.isAvailable && 'cursor-not-allowed opacity-50')}>
                    <input
                      type={single ? 'radio' : 'checkbox'}
                      name={`${id}-${g.id}`}
                      checked={checked}
                      disabled={!o.isAvailable}
                      onChange={() => toggle(g.id, o.id, single)}
                      className="size-5 accent-[var(--c-accent)]"
                    />
                    <span className="flex-1">{o.name}</span>
                    {!o.isAvailable ? <span className="t-small text-muted">{t('unavailableOption')}</span> : o.priceDelta ? <span className="t-small tabular text-muted"><bdi>+{formatMoney(o.priceDelta, locale)}</bdi></span> : null}
                  </label>
                );
              })}
            </fieldset>
          );
        })}

        <div>
          <Label htmlFor={`${id}-note`}>{t('note')}</Label>
          <Textarea id={`${id}-note`} value={note} maxLength={140} rows={2} className="min-h-20" placeholder={t('notePlaceholder')} onChange={(e) => setNote(e.target.value)} />
        </div>
        {error ? (
          <p role="alert" className="t-small text-danger">
            {error}
          </p>
        ) : null}
      </div>
    </Dialog>
  );
}
