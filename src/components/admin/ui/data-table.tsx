'use client';

import {
  flexRender,
  getCoreRowModel,
  getFilteredRowModel,
  getPaginationRowModel,
  getSortedRowModel,
  useReactTable,
  type ColumnDef,
  type SortingState,
} from '@tanstack/react-table';
import { ArrowDown, ArrowUp, ChevronLeft, ChevronRight, Search } from 'lucide-react';
import { useLocale, useTranslations } from 'next-intl';
import { useRouter } from 'next/navigation';
import { useId, useState, type ReactNode } from 'react';
import { matchesSearch } from '@/lib/i18n/arabic';
import { formatNumber } from '@/lib/i18n/format';
import { cn } from '@/lib/utils/cn';
import { Button } from './button';
import { Input } from './input';
import { EmptyState } from './page-header';
import { Table, TableWrap, TBody, TD, TH, THead, TR } from './table';

export type { ColumnDef };

/**
 * Sortable, searchable, paginated table (TanStack Table). Search is accent- and hamza-insensitive in Arabic.
 * Rows can open a record: the row is clickable for pointers, and the record's main cell should render a link
 * so keyboard and screen-reader users reach it too.
 */
export function DataTable<T>({
  data,
  columns,
  searchText,
  searchLabel,
  rowHref,
  toolbar,
  empty,
  pageSize = 25,
  initialSort,
  className,
  dense,
}: {
  data: T[];
  columns: ColumnDef<T, unknown>[];
  searchText?: (row: T) => string;
  searchLabel?: string;
  rowHref?: (row: T) => string | null;
  toolbar?: ReactNode;
  empty?: { title: ReactNode; body?: ReactNode; icon?: ReactNode };
  pageSize?: number;
  initialSort?: SortingState;
  className?: string;
  dense?: boolean;
}) {
  const t = useTranslations('admin.ui.table');
  const locale = useLocale();
  const router = useRouter();
  const id = useId();
  const [sorting, setSorting] = useState<SortingState>(initialSort ?? []);
  const [query, setQuery] = useState('');

  // TanStack Table's instance is mutable by design, so the React Compiler leaves this component unmemoised (intended).
  // eslint-disable-next-line react-hooks/incompatible-library
  const table = useReactTable({
    data,
    columns,
    state: { sorting, globalFilter: query },
    onSortingChange: setSorting,
    onGlobalFilterChange: setQuery,
    globalFilterFn: (row, _column, value: string) => (searchText ? matchesSearch(searchText(row.original), value) : true),
    getCoreRowModel: getCoreRowModel(),
    getSortedRowModel: getSortedRowModel(),
    getFilteredRowModel: getFilteredRowModel(),
    getPaginationRowModel: getPaginationRowModel(),
    initialState: { pagination: { pageSize } },
  });

  const rows = table.getRowModel().rows;
  const total = table.getFilteredRowModel().rows.length;
  const { pageIndex } = table.getState().pagination;
  const pages = table.getPageCount();
  const fmt = (n: number) => formatNumber(n, locale);

  return (
    <div className={cn('flex flex-col', className)}>
      {searchText || toolbar ? (
        <div className="flex flex-wrap items-center gap-2 px-5 py-3">
          {searchText ? (
            <div className="relative w-full max-w-xs">
              <label htmlFor={`${id}-q`} className="sr-only">
                {searchLabel ?? t('search')}
              </label>
              <Search className="pointer-events-none absolute inset-y-0 start-2.5 my-auto size-4 text-muted" aria-hidden="true" />
              <Input
                id={`${id}-q`}
                type="search"
                data-table-search
                value={query}
                onChange={(e) => {
                  setQuery(e.target.value);
                  table.setPageIndex(0);
                }}
                placeholder={searchLabel ?? t('search')}
                className="ps-8"
              />
            </div>
          ) : null}
          {toolbar ? <div className="flex flex-1 flex-wrap items-center gap-2">{toolbar}</div> : null}
          <p className="ms-auto text-xs text-muted tabular" aria-live="polite">
            {t('count', { shown: fmt(total), total: fmt(data.length) })}
          </p>
        </div>
      ) : null}
      {rows.length ? (
        <TableWrap>
          <Table>
            <THead>
              {table.getHeaderGroups().map((group) => (
                <tr key={group.id}>
                  {group.headers.map((header) => {
                    const sortable = header.column.getCanSort();
                    const dir = header.column.getIsSorted();
                    const meta = header.column.columnDef.meta as { align?: 'end'; className?: string } | undefined;
                    return (
                      <TH key={header.id} aria-sort={dir === 'asc' ? 'ascending' : dir === 'desc' ? 'descending' : sortable ? 'none' : undefined} className={cn(meta?.align === 'end' && 'text-end', meta?.className)}>
                        {header.isPlaceholder ? null : sortable ? (
                          <button type="button" onClick={header.column.getToggleSortingHandler()} className={cn('inline-flex items-center gap-1 rounded-hair hover-capable:hover:text-ink', meta?.align === 'end' && 'flex-row-reverse')}>
                            {flexRender(header.column.columnDef.header, header.getContext())}
                            {dir === 'asc' ? <ArrowUp className="size-3" aria-hidden="true" /> : dir === 'desc' ? <ArrowDown className="size-3" aria-hidden="true" /> : null}
                          </button>
                        ) : (
                          flexRender(header.column.columnDef.header, header.getContext())
                        )}
                      </TH>
                    );
                  })}
                </tr>
              ))}
            </THead>
            <TBody>
              {rows.map((row) => {
                const href = rowHref?.(row.original) ?? null;
                return (
                  <TR
                    key={row.id}
                    data-clickable={href ? true : undefined}
                    onClick={
                      href
                        ? (e) => {
                            if ((e.target as HTMLElement).closest('a,button,input,select,textarea,label,[role="menuitem"]')) return;
                            if (e.metaKey || e.ctrlKey) window.open(href, '_blank', 'noopener');
                            else router.push(href);
                          }
                        : undefined
                    }
                  >
                    {row.getVisibleCells().map((cell) => {
                      const meta = cell.column.columnDef.meta as { align?: 'end'; className?: string } | undefined;
                      return (
                        <TD key={cell.id} className={cn(dense && 'py-1.5', meta?.align === 'end' && 'text-end tabular', meta?.className)}>
                          {flexRender(cell.column.columnDef.cell, cell.getContext())}
                        </TD>
                      );
                    })}
                  </TR>
                );
              })}
            </TBody>
          </Table>
        </TableWrap>
      ) : (
        <EmptyState icon={empty?.icon} title={query ? t('noMatches') : (empty?.title ?? t('empty'))} body={query ? t('noMatchesBody') : empty?.body} />
      )}
      {pages > 1 ? (
        <div className="flex items-center justify-between gap-2 border-t border-line px-5 py-2.5">
          <p className="text-xs text-muted tabular">{t('page', { page: fmt(pageIndex + 1), pages: fmt(pages) })}</p>
          <div className="flex gap-1">
            <Button size="icon-sm" variant="ghost" onClick={() => table.previousPage()} disabled={!table.getCanPreviousPage()} aria-label={t('previous')}>
              <ChevronLeft className="rtl:-scale-x-100" />
            </Button>
            <Button size="icon-sm" variant="ghost" onClick={() => table.nextPage()} disabled={!table.getCanNextPage()} aria-label={t('next')}>
              <ChevronRight className="rtl:-scale-x-100" />
            </Button>
          </div>
        </div>
      ) : null}
    </div>
  );
}
