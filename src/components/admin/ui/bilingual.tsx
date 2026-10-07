'use client';

import { useTranslations } from 'next-intl';
import type { ReactNode } from 'react';
import type { LocalizedText } from '@/lib/i18n/localized';
import { cn } from '@/lib/utils/cn';
import { FieldError, Hint } from './field';
import { Input, Textarea } from './input';

type Lang = 'ar' | 'en';
const LANGS: Lang[] = ['ar', 'en'];

/**
 * One text in both languages, side by side on wide screens: Arabic right-to-left, English left-to-right,
 * each with its own label so screen readers announce the language being edited.
 */
export function BilingualField({
  id,
  label,
  value,
  onChange,
  multiline,
  rows = 3,
  required,
  hint,
  error,
  maxLength,
  className,
  name,
}: {
  id: string;
  label: ReactNode;
  value: LocalizedText | null | undefined;
  onChange: (value: LocalizedText) => void;
  multiline?: boolean;
  rows?: number;
  required?: boolean | Lang[];
  hint?: ReactNode;
  error?: Partial<Record<Lang, string>> | string | null;
  maxLength?: number;
  className?: string;
  name?: string;
}) {
  const t = useTranslations('admin.ui.languages');
  const current = value ?? {};
  const errorFor = (lang: Lang) => (typeof error === 'string' ? (lang === 'ar' ? error : undefined) : error?.[lang]);
  const isRequired = (lang: Lang) => (Array.isArray(required) ? required.includes(lang) : Boolean(required));
  return (
    <fieldset className={className}>
      <legend className="mb-1.5 text-[0.8125rem] font-medium">{label}</legend>
      <div className="grid gap-2 md:grid-cols-2">
        {LANGS.map((lang) => {
          const fieldId = `${id}-${lang}`;
          const err = errorFor(lang);
          const common = {
            id: fieldId,
            name: name ? `${name}.${lang}` : undefined,
            lang,
            dir: lang === 'ar' ? 'rtl' : 'ltr',
            value: current[lang] ?? '',
            maxLength,
            required: isRequired(lang),
            'aria-invalid': err ? (true as const) : undefined,
            'aria-describedby': err ? `${fieldId}-error` : undefined,
            onChange: (e: { target: { value: string } }) => onChange({ ...current, [lang]: e.target.value }),
          };
          return (
            <div key={lang} className="min-w-0">
              <label htmlFor={fieldId} className="mb-1 flex items-center gap-1.5 text-xs text-muted">
                <span className="rounded-hair border border-line px-1 font-mono text-[0.625rem] uppercase" aria-hidden="true">
                  {lang}
                </span>
                {t(lang)}
                {isRequired(lang) ? (
                  <span aria-hidden="true" className="text-accent">
                    *
                  </span>
                ) : null}
              </label>
              {multiline ? <Textarea rows={rows} {...common} className={cn(lang === 'ar' && 'leading-loose')} /> : <Input {...common} />}
              <FieldError id={`${fieldId}-error`}>{err}</FieldError>
            </div>
          );
        })}
      </div>
      {hint ? <Hint>{hint}</Hint> : null}
    </fieldset>
  );
}
