import type { ContextReport, ProviderReport, UsageReport } from '@waku/client'
import {
  breakdownSegments,
  estimateMark,
  formatTokens,
  providerResetPhrase,
  usedPercent,
  type BreakdownPart,
} from '@/lib/context-report'
import { useI18n } from '@/lib/i18n'
import type { Translator } from '@/lib/transcript-presentation'
import { cn } from '@/lib/utils'

const CATEGORY_HUES = [209, 270, 29, 151, 331, 54, 180, 108]

/** A category's swatch color. Rows always name their category, so the hue
 * only links a row to its bar segment. */
function partColor(part: BreakdownPart) {
  switch (part.kind) {
    case 'category':
      return `hsl(${CATEGORY_HUES[part.index % CATEGORY_HUES.length]} 55% 56%)`
    case 'free':
      return 'var(--accent)'
    case 'reserve':
      return 'var(--text-tertiary)'
  }
}

export function hasEstimate(context: ContextReport, estimated: boolean) {
  return estimated || context.categories.some((category) => category.estimated)
}

/** The stacked context bar plus one labeled row per segment. Shared by the
 * usage popover and the `/context` transcript card. */
export function ContextBreakdown({ context, t }: { context: ContextReport; t: Translator }) {
  const segments = breakdownSegments(context)
  const window = Math.max(context.contextWindow, 1)
  return (
    <div className="flex flex-col gap-1.5">
      <div
        aria-hidden="true"
        className="flex h-1.5 w-full overflow-hidden rounded-full bg-accent"
      >
        {segments.map((segment, index) => (
          <div
            key={index}
            className="h-full shrink-0"
            style={{ width: `${segment.fraction * 100}%`, background: partColor(segment.part) }}
          />
        ))}
      </div>
      {segments.map((segment, index) => {
        const category = segment.part.kind === 'category'
          ? context.categories[segment.part.index]
          : undefined
        const label = category?.label
          ?? (segment.part.kind === 'free' ? t('usage.context_free') : t('usage.context_reserve'))
        const mark = estimateMark(category?.estimated ?? false)
        const percent = segment.tokens * 100 / window
        return (
          <div key={index} className="flex min-w-0 items-center gap-2">
            <span
              aria-hidden="true"
              className="size-2 shrink-0 rounded-[2px]"
              style={{ background: partColor(segment.part) }}
            />
            <span className="min-w-0 flex-1 truncate text-[var(--text-secondary)]">{label}</span>
            <span className="shrink-0 text-[11px] text-[var(--text-tertiary)]">
              {`${mark}${formatTokens(segment.tokens)} · ${mark}${percent.toFixed(1)}%`}
            </span>
          </div>
        )
      })}
    </div>
  )
}

export function EstimateHint({ t }: { t: Translator }) {
  return <div className="text-[11px] text-[var(--text-tertiary)]">{t('usage.context_estimate_hint')}</div>
}

/** A provider report (choco-pi's `/status`, `/context`, `/usage`) as a
 * transcript card: the context occupancy and breakdown first, then each
 * section as a label/value table. A usage report's typed block replaces its
 * sections, which repeat the same figures as text. */
export function ReportCard({ report, t }: { report: ProviderReport; t: Translator }) {
  const context = report.kind === 'context' ? report.context : null
  const usage = report.kind === 'usage' ? report.usage : null
  const occupied = context
    ? context.measuredTokens ?? context.estimatedTokens
    : 0
  const estimated = context ? context.measuredTokens == null : false
  const mark = estimateMark(estimated)
  return (
    <section
      aria-label={report.title}
      className="flex w-full max-w-[560px] flex-col gap-2.5 rounded-[10px] border border-border bg-accent/40 px-3.5 py-3 text-xs leading-[17px]"
    >
      <header className="flex min-w-0 items-center gap-2">
        <h3 className="shrink-0 font-semibold">{report.title}</h3>
        <span className="flex-1" />
        {report.context?.model && (
          <span className="min-w-0 truncate text-[var(--text-tertiary)]">{report.context.model}</span>
        )}
      </header>
      {context && (
        <>
          <div className="text-[var(--text-secondary)]">
            {`${mark}${formatTokens(occupied)} / ${formatTokens(context.contextWindow)} (${mark}${(
              occupied * 100 / Math.max(context.contextWindow, 1)
            ).toFixed(0)}%)`}
          </div>
          <ContextBreakdown context={context} t={t} />
          {hasEstimate(context, estimated) && <EstimateHint t={t} />}
        </>
      )}
      {usage && <UsageBlocks t={t} usage={usage} />}
      {!usage && report.sections.map((section, index) => (
        <table key={`${section.title}-${index}`} className="w-full table-fixed border-collapse">
          {section.title.trim() && (
            <caption className="pb-0.5 text-left text-[11px] font-medium text-[var(--text-tertiary)]">
              {section.title}
            </caption>
          )}
          <tbody>
            {section.rows.map((row, rowIndex) => (
              <tr key={`${row.label}-${rowIndex}`} className="align-top">
                <th className="w-[38%] py-px pr-3 text-left font-normal text-[var(--text-secondary)]" scope="row">
                  {row.label}
                </th>
                <td className="py-px break-words">
                  <div>{row.value}</div>
                  {row.hint && <div className="text-[11px] text-[var(--text-tertiary)]">{row.hint}</div>}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      ))}
    </section>
  )
}

export function UsageLane({
  label,
  percent,
  reset,
  value,
}: {
  label: string
  percent: number
  reset?: string
  value: string
}) {
  return (
    <div className="space-y-[7px]">
      <div className="flex min-w-0 items-center gap-2">
        <span className="min-w-0 flex-1 truncate">{label}</span>
        {reset && <span className="shrink-0 text-[11px] text-[var(--text-tertiary)]">{reset}</span>}
        <span className="shrink-0 text-[11px] text-[var(--text-secondary)]">{value}</span>
      </div>
      <div className="h-[3px] overflow-hidden rounded-full bg-accent">
        <div
          className={cn(
            'h-full rounded-full bg-[var(--text-secondary)]',
            percent >= 80 && 'bg-[var(--warning)]',
            percent >= 95 && 'bg-destructive',
          )}
          style={{ width: `${Math.min(100, Math.max(0, percent))}%` }}
        />
      </div>
    </div>
  )
}

export function resetLabel(resetsAt: number, locale: string, t: Translator) {
  const seconds = resetsAt - Math.floor(Date.now() / 1_000)
  if (seconds <= 0) return t('usage.resets_soon')
  const minutes = Math.ceil(seconds / 60)
  if (minutes < 60) return t('usage.resets_in_minutes', { count: minutes })
  if (minutes < 24 * 60) {
    const hours = Math.floor(minutes / 60)
    const remainder = minutes % 60
    return remainder
      ? t('usage.resets_in_hours_minutes', { hours, minutes: remainder })
      : t('usage.resets_in_hours', { count: hours })
  }
  return t('usage.resets_date', { date: new Intl.DateTimeFormat(locale, {
    weekday: 'short',
    hour: 'numeric',
    minute: '2-digit',
  }).format(new Date(resetsAt * 1_000)) })
}

/** A `usage` report's providers: a heading with plan, status, and cache
 * note, then one quota lane per window, always read as percent used. */
function UsageBlocks({ usage, t }: { usage: UsageReport; t: Translator }) {
  const { locale } = useI18n()
  const now = Math.floor(Date.now() / 1_000)
  return (
    <div className="flex flex-col gap-3">
      {usage.providers.map((provider, index) => {
        const summary = [provider.plan, provider.status]
          .map((part) => part?.trim())
          .filter(Boolean)
          .join(' \u00b7 ')
        const reason = provider.cached?.reason?.trim()
        return (
          <div key={`${provider.name}-${index}`} className="flex flex-col gap-[7px]">
            <div className="flex min-w-0 items-center gap-2">
              <span className="shrink-0 font-medium">{provider.name}</span>
              <span className="min-w-0 flex-1 truncate text-[var(--text-secondary)]">{summary}</span>
            </div>
            {provider.cached && (
              <div className="text-[11px] text-[var(--text-tertiary)]">
                {reason ? `${t('usage.cached_snapshot')} \u00b7 ${reason}` : t('usage.cached_snapshot')}
              </div>
            )}
            {provider.windows.map((window, windowIndex) => {
              const used = usedPercent(window)
              const resetsAt = window.resetsAt == null ? null : Math.floor(window.resetsAt / 1_000)
              const reset = resetsAt == null
                ? undefined
                : providerResetPhrase(window.resetLabel, resetsAt, now) ?? resetLabel(resetsAt, locale, t)
              const detail = [window.pace?.label, window.detail]
                .map((part) => part?.trim())
                .filter(Boolean)
                .join(' \u00b7 ')
              return (
                <div key={`${window.label}-${windowIndex}`} className="flex flex-col gap-1">
                  <UsageLane label={window.label} percent={used} reset={reset} value={`${used.toFixed(0)}%`} />
                  {detail && <div className="text-[11px] text-[var(--text-tertiary)]">{detail}</div>}
                </div>
              )
            })}
          </div>
        )
      })}
    </div>
  )
}
