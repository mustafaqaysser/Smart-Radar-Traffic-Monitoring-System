'use client';

import { useLocale } from 'next-intl';
import { useState, type ComponentProps } from 'react';
import { normalizeDigits } from '@/lib/i18n/digits';
import { formatAmount, formatNumber } from '@/lib/i18n/format';
import { cn } from '@/lib/utils/cn';
import { Input } from './input';

/**
 * Money typed in major units (riyals, with any digits — Arabic-Indic included) and stored as minor units (halalas).
 * Shows the currency beside the field; emits null while the text is not a valid amount.
 */
export function MoneyInput({ value, onChange, currencyLabel, className, ...props }: Omit<ComponentProps<'input'>, 'value' | 'onChange' | 'type'> & { value: number | null; onChange: (minor: number | null) => void; currencyLabel: string }) {
  const locale = useLocale();
  const [text, setText] = useState(() => (value === null ? '' : formatAmount(value, 'en').replace(/,/g, '')));
  const [focused, setFocused] = useState(false);
  const shown = focused || value === null ? text : formatAmount(value, locale);
  return (
    <div className="relative">
      <Input
        {...props}
        inputMode="decimal"
        dir="ltr"
        value={shown}
        onFocus={(e) => {
          setFocused(true);
          setText(value === null ? text : formatAmount(value, 'en').replace(/,/g, ''));
          props.onFocus?.(e);
        }}
        onBlur={(e) => {
          setFocused(false);
          props.onBlur?.(e);
        }}
        onChange={(e) => {
          const raw = e.target.value;
          setText(raw);
          const clean = normalizeDigits(raw).replace(/[,\s]/g, '');
          if (!clean) return onChange(null);
          if (!/^\d+(\.\d{0,2})?$/.test(clean)) return onChange(null);
          onChange(Math.round(Number(clean) * 100));
        }}
        className={cn('pe-14 text-end tabular rtl:text-end', className)}
      />
      <span className="pointer-events-none absolute inset-y-0 end-3 flex items-center text-xs text-muted">{currencyLabel}</span>
    </div>
  );
}

/** Whole numbers typed in any digits. Emits null when empty or invalid. */
export function IntegerInput({ value, onChange, className, suffix, ...props }: Omit<ComponentProps<'input'>, 'value' | 'onChange' | 'type'> & { value: number | null; onChange: (n: number | null) => void; suffix?: string }) {
  const locale = useLocale();
  const [text, setText] = useState(value === null ? '' : String(value));
  const [focused, setFocused] = useState(false);
  const shown = focused || value === null ? text : formatNumber(value, locale, { useGrouping: false });
  return (
    <div className="relative">
      <Input
        {...props}
        inputMode="numeric"
        dir="ltr"
        value={shown}
        onFocus={(e) => {
          setFocused(true);
          setText(value === null ? text : String(value));
          props.onFocus?.(e);
        }}
        onBlur={(e) => {
          setFocused(false);
          props.onBlur?.(e);
        }}
        onChange={(e) => {
          setText(e.target.value);
          const clean = normalizeDigits(e.target.value).trim();
          onChange(/^-?\d+$/.test(clean) ? Number(clean) : null);
        }}
        className={cn('tabular', suffix && 'pe-14', className)}
      />
      {suffix ? <span className="pointer-events-none absolute inset-y-0 end-3 flex items-center text-xs text-muted">{suffix}</span> : null}
    </div>
  );
}
