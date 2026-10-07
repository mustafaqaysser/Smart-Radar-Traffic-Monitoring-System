'use client';

import { useLocale, useTranslations } from 'next-intl';
import { useMemo, useState, useTransition } from 'react';
import { Icon } from '@/components/brand/icon';
import { Button, ButtonLink } from '@/components/site/ui/button';
import { Chip } from '@/components/site/ui/field';
import { toast } from '@/components/site/ui/toast';
import { useRouter } from '@/i18n/navigation';
import { saveDietaryProfile } from '@/lib/actions/account';
import { formatNumber } from '@/lib/i18n/format';
import { profileVerdict, type DietProfile } from '@/lib/menu/filter';
import { ALLERGENS, DIETARY_TAGS, type Allergen, type DietaryTag } from '@/lib/menu/tags';

type Tags = { dietary: DietaryTag[]; allergens: Allergen[]; spice: number };

/** Diets, allergens and heat, with a live count of how much of today's menu the profile leaves open. */
export function DietaryForm({ initial, dishes }: { initial: DietProfile | null; dishes: Tags[] }) {
  const t = useTranslations('account.dietary');
  const tc = useTranslations('common');
  const tf = useTranslations('forms');
  const locale = useLocale();
  const router = useRouter();
  const [diets, setDiets] = useState<DietaryTag[]>(initial?.diets ?? []);
  const [avoid, setAvoid] = useState<Allergen[]>(initial?.avoid ?? []);
  const [maxSpice, setMaxSpice] = useState<number | null>(initial?.maxSpice ?? null);
  const [pending, start] = useTransition();
  const profile: DietProfile = { diets, avoid, maxSpice };
  const suits = useMemo(() => dishes.filter((d) => profileVerdict(d, { diets, avoid, maxSpice }).suits).length, [dishes, diets, avoid, maxSpice]);
  const toggle = <T,>(list: T[], v: T) => (list.includes(v) ? list.filter((x) => x !== v) : [...list, v]);

  const save = (next: DietProfile) =>
    start(async () => {
      const res = await saveDietaryProfile(next);
      if (!res.ok) {
        toast(tf.has(`errors.${res.error}`) ? tf(`errors.${res.error}`) : tf('errors.unknown'));
        return;
      }
      toast(res.data.empty ? t('cleared') : t('saved'));
      router.refresh();
    });

  return (
    <div className="flex flex-col gap-10">
      <fieldset className="flex flex-col gap-3">
        <legend className="t-label mb-3 text-muted">{t('diets')}</legend>
        <div className="flex flex-wrap gap-2">
          {DIETARY_TAGS.map((d) => (
            <Chip key={d} pressed={diets.includes(d)} onClick={() => setDiets((l) => toggle(l, d))}>
              {tc(`dietary.${d}`)}
            </Chip>
          ))}
        </div>
      </fieldset>
      <fieldset className="flex flex-col gap-3">
        <legend className="t-label mb-1 text-muted">{t('avoid')}</legend>
        <p className="t-small mb-3 text-muted">{t('avoidHint')}</p>
        <div className="flex flex-wrap gap-2">
          {ALLERGENS.map((a) => (
            <Chip key={a} pressed={avoid.includes(a)} onClick={() => setAvoid((l) => toggle(l, a))} className="px-3">
              <Icon name={a} size={16} struck={avoid.includes(a)} />
              {tc(`allergens.${a}`)}
            </Chip>
          ))}
        </div>
      </fieldset>
      <fieldset className="flex flex-col gap-3">
        <legend className="t-label mb-3 text-muted">{t('spice')}</legend>
        <div className="flex flex-wrap gap-2">
          <Chip pressed={maxSpice === null} onClick={() => setMaxSpice(null)}>
            {t('spiceAny')}
          </Chip>
          {[0, 1, 2].map((s) => (
            <Chip key={s} pressed={maxSpice === s} onClick={() => setMaxSpice(s)}>
              {s === 0 ? tc('spice.0') : t('spiceUpTo', { level: tc(`spice.${s}`) })}
            </Chip>
          ))}
        </div>
      </fieldset>
      {dishes.length ? (
        <p className="t-body flex items-center gap-3 border-t border-line pt-6" aria-live="polite">
          <Icon name="leaf" size={20} className="text-accent" />
          {t('preview', { suits: formatNumber(suits, locale), total: formatNumber(dishes.length, locale) })}
        </p>
      ) : null}
      <div className="flex flex-wrap items-center gap-x-6 gap-y-4">
        <Button icon={null} disabled={pending} onClick={() => save(profile)}>
          {pending ? t('saving') : t('save')}
        </Button>
        {initial ? (
          <button
            type="button"
            disabled={pending}
            className="t-small underline decoration-line underline-offset-4"
            onClick={() => {
              setDiets([]);
              setAvoid([]);
              setMaxSpice(null);
              save({ diets: [], avoid: [], maxSpice: null });
            }}
          >
            {t('clear')}
          </button>
        ) : null}
        <ButtonLink href="/menu" variant="quiet" size="sm">
          {t('seeMenu')}
        </ButtonLink>
      </div>
    </div>
  );
}
