'use client';

import { useLocale } from 'next-intl';
import type { ReactNode } from 'react';
import { Area, AreaChart, Bar, BarChart, CartesianGrid, ResponsiveContainer, Tooltip, XAxis, YAxis } from 'recharts';
import { formatAmount, formatDateString, formatMoney, formatNumber, formatWallTime } from '@/lib/i18n/format';

/**
 * Charts in the brand's palette. In Arabic the time axis runs right to left and the value axis sits on the right,
 * the way an Arabic reader scans; digits follow the configured numbering system.
 */

const axis = { stroke: 'var(--c-line)', tick: { fill: 'var(--c-muted)', fontSize: 11 }, tickLine: false };

function ChartTooltip({ title, rows }: { title: string; rows: { label: string; value: string; color?: string }[] }) {
  return (
    <div className="cast rounded-soft border border-line bg-raised px-3 py-2 text-xs">
      <p className="mb-1 font-medium">{title}</p>
      {rows.map((r) => (
        <p key={r.label} className="flex items-center gap-2 text-muted">
          {r.color ? <span className="size-2 rounded-full" style={{ background: r.color }} aria-hidden="true" /> : null}
          <span>{r.label}</span>
          <span className="ms-auto font-medium text-ink tabular">{r.value}</span>
        </p>
      ))}
    </div>
  );
}

/** Screen readers get the same figures as a table; the drawing itself is hidden from them. */
function Figure({ label, height, head, rows, children }: { label: string; height: number; head: string[]; rows: string[][]; children: ReactNode }) {
  return (
    <figure>
      <figcaption className="sr-only">{label}</figcaption>
      <div aria-hidden="true" style={{ height }} className="w-full" dir="ltr">
        {children}
      </div>
      <table className="sr-only">
        <thead>
          <tr>
            {head.map((h) => (
              <th key={h} scope="col">
                {h}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {rows.map((r) => (
            <tr key={r[0]}>
              {r.map((cell, i) => (i === 0 ? <th key={i} scope="row">{cell}</th> : <td key={i}>{cell}</td>))}
            </tr>
          ))}
        </tbody>
      </table>
    </figure>
  );
}

export function SalesChart({ data, labels }: { data: { date: string; revenue: number; orders: number }[]; labels: { title: string; date: string; revenue: string; orders: string } }) {
  const locale = useLocale();
  const rtl = locale === 'ar';
  return (
    <Figure
      label={labels.title}
      height={220}
      head={[labels.date, labels.revenue, labels.orders]}
      rows={data.map((d) => [formatDateString(d.date, locale, { day: 'numeric', month: 'long' }), formatMoney(d.revenue, locale), formatNumber(d.orders, locale)])}
    >
      <ResponsiveContainer width="100%" height="100%">
        <BarChart data={data} margin={{ top: 8, right: 4, left: 4, bottom: 0 }}>
          <CartesianGrid vertical={false} stroke="var(--c-line)" />
          <XAxis dataKey="date" reversed={rtl} {...axis} tickFormatter={(d: string) => formatDateString(d, locale, { day: 'numeric' })} interval="preserveStartEnd" minTickGap={8} />
          <YAxis orientation={rtl ? 'right' : 'left'} {...axis} axisLine={false} width={48} tickFormatter={(v: number) => formatAmount(Math.round(v / 100) * 100, locale)} />
          <Tooltip
            cursor={{ fill: 'var(--c-surface)' }}
            content={({ active, payload }) => {
              const row = active ? (payload?.[0]?.payload as (typeof data)[number] | undefined) : undefined;
              if (!row) return null;
              return (
                <ChartTooltip
                  title={formatDateString(row.date, locale, { weekday: 'long', day: 'numeric', month: 'long' })}
                  rows={[
                    { label: labels.revenue, value: formatMoney(row.revenue, locale), color: 'var(--c-accent)' },
                    { label: labels.orders, value: formatNumber(row.orders, locale) },
                  ]}
                />
              );
            }}
          />
          <Bar dataKey="revenue" fill="var(--c-accent)" radius={[2, 2, 0, 0]} maxBarSize={28} isAnimationActive={false} />
        </BarChart>
      </ResponsiveContainer>
    </Figure>
  );
}

export function HourlyChart({ data, labels }: { data: { hour: number; today: number; lastWeek: number }[]; labels: { title: string; hour: string; today: string; lastWeek: string } }) {
  const locale = useLocale();
  const rtl = locale === 'ar';
  const hourLabel = (h: number) => formatWallTime(`${String(h).padStart(2, '0')}:00`, locale);
  return (
    <Figure
      label={labels.title}
      height={200}
      head={[labels.hour, labels.today, labels.lastWeek]}
      rows={data.map((d) => [hourLabel(d.hour), formatNumber(d.today, locale), formatNumber(d.lastWeek, locale)])}
    >
      <ResponsiveContainer width="100%" height="100%">
        <AreaChart data={data} margin={{ top: 8, right: 4, left: 4, bottom: 0 }}>
          <CartesianGrid vertical={false} stroke="var(--c-line)" />
          <XAxis dataKey="hour" reversed={rtl} {...axis} tickFormatter={hourLabel} interval={3} />
          <YAxis orientation={rtl ? 'right' : 'left'} {...axis} axisLine={false} width={32} allowDecimals={false} tickFormatter={(v: number) => formatNumber(v, locale)} />
          <Tooltip
            content={({ active, payload }) => {
              const row = active ? (payload?.[0]?.payload as (typeof data)[number] | undefined) : undefined;
              if (!row) return null;
              return (
                <ChartTooltip
                  title={hourLabel(row.hour)}
                  rows={[
                    { label: labels.today, value: formatNumber(row.today, locale), color: 'var(--c-accent)' },
                    { label: labels.lastWeek, value: formatNumber(row.lastWeek, locale), color: 'var(--c-muted)' },
                  ]}
                />
              );
            }}
          />
          <Area type="monotone" dataKey="lastWeek" stroke="var(--c-muted)" strokeDasharray="4 4" fill="transparent" strokeWidth={1.5} isAnimationActive={false} />
          <Area type="monotone" dataKey="today" stroke="var(--c-accent)" fill="var(--c-accent)" fillOpacity={0.12} strokeWidth={2} isAnimationActive={false} />
        </AreaChart>
      </ResponsiveContainer>
    </Figure>
  );
}

export function VisitorsChart({ data, labels }: { data: { date: string; views: number; visitors: number }[]; labels: { title: string; date: string; views: string; visitors: string } }) {
  const locale = useLocale();
  const rtl = locale === 'ar';
  return (
    <Figure
      label={labels.title}
      height={180}
      head={[labels.date, labels.views, labels.visitors]}
      rows={data.map((d) => [formatDateString(d.date, locale, { day: 'numeric', month: 'long' }), formatNumber(d.views, locale), formatNumber(d.visitors, locale)])}
    >
      <ResponsiveContainer width="100%" height="100%">
        <AreaChart data={data} margin={{ top: 8, right: 4, left: 4, bottom: 0 }}>
          <CartesianGrid vertical={false} stroke="var(--c-line)" />
          <XAxis dataKey="date" reversed={rtl} {...axis} tickFormatter={(d: string) => formatDateString(d, locale, { day: 'numeric' })} interval="preserveStartEnd" minTickGap={8} />
          <YAxis orientation={rtl ? 'right' : 'left'} {...axis} axisLine={false} width={36} allowDecimals={false} tickFormatter={(v: number) => formatNumber(v, locale)} />
          <Tooltip
            content={({ active, payload }) => {
              const row = active ? (payload?.[0]?.payload as (typeof data)[number] | undefined) : undefined;
              if (!row) return null;
              return (
                <ChartTooltip
                  title={formatDateString(row.date, locale, { weekday: 'long', day: 'numeric', month: 'long' })}
                  rows={[
                    { label: labels.views, value: formatNumber(row.views, locale), color: 'var(--c-sun)' },
                    { label: labels.visitors, value: formatNumber(row.visitors, locale), color: 'var(--c-ink)' },
                  ]}
                />
              );
            }}
          />
          <Area type="monotone" dataKey="views" stroke="var(--c-sun)" fill="var(--c-sun)" fillOpacity={0.18} strokeWidth={1.5} isAnimationActive={false} />
          <Area type="monotone" dataKey="visitors" stroke="var(--c-ink)" fill="transparent" strokeWidth={1.5} isAnimationActive={false} />
        </AreaChart>
      </ResponsiveContainer>
    </Figure>
  );
}

/** A labelled share bar (no chart library needed for proportions). */
export function ShareBars({ rows }: { rows: { label: string; value: number; display: string; note?: string }[] }) {
  const total = rows.reduce((sum, r) => sum + r.value, 0) || 1;
  return (
    <ul className="flex flex-col gap-3">
      {rows.map((r) => (
        <li key={r.label}>
          <div className="mb-1 flex items-baseline justify-between gap-3 text-[0.8125rem]">
            <span>{r.label}</span>
            <span className="tabular">
              {r.display}
              {r.note ? <span className="ms-2 text-xs text-muted">{r.note}</span> : null}
            </span>
          </div>
          <div className="h-1.5 overflow-hidden rounded-pill bg-surface" aria-hidden="true">
            <div className="h-full rounded-pill bg-accent" style={{ width: `${(r.value / total) * 100}%` }} />
          </div>
        </li>
      ))}
    </ul>
  );
}
