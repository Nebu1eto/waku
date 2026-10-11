import type { ContextReport, ProviderReport } from '@waku/client'
import {
  breakdownSegments,
  estimateMark,
  formatTokens,
  type BreakdownPart,
} from '@/lib/context-report'
import type { Translator } from '@/lib/transcript-presentation'

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

/** A provider report (choco-pi's `/status`, `/context`) as a transcript
 * card: the context occupancy and breakdown first, then each section as a
 * label/value table. */
export function ReportCard({ report, t }: { report: ProviderReport; t: Translator }) {
  const context = report.kind === 'context' ? report.context : null
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
      {report.sections.map((section, index) => (
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
