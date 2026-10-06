'use client';

import { useEffect, useRef } from 'react';

/**
 * Invisible bot defences rendered in every public form: a honeypot that humans never see or fill, and the
 * time the form became interactive (submissions faster than a person could type are rejected).
 */
export function BotFields() {
  const rendered = useRef<HTMLInputElement>(null);
  useEffect(() => {
    if (rendered.current) rendered.current.value = String(Date.now());
  }, []);
  return (
    <div aria-hidden="true" className="absolute -start-[10000px] h-px w-px overflow-hidden">
      <label>
        Website
        <input type="text" name="company_website" tabIndex={-1} autoComplete="off" defaultValue="" />
      </label>
      <input ref={rendered} type="hidden" name="rendered_at" defaultValue="" />
    </div>
  );
}
