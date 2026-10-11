import type { ContextReport, ContextUsage, ProviderReport, ProviderReportRow } from '@waku/client'

export function formatTokens(tokens: number) {
  if (tokens >= 999_500) return `${(tokens / 1_000_000).toFixed(1)}M`
  return tokens >= 1_000 ? `${(tokens / 1_000).toFixed(1)}k` : `${tokens}`
}

/** "≈" before a number the provider estimated rather than measured. */
export function estimateMark(estimated: boolean) {
  return estimated ? '\u2248' : ''
}

/**
 * The meter's occupancy and whether it is the provider's estimate. Mirrors
 * the desktop's `meter_usage`: a fresh choco-pi session has measured nothing
 * yet, so its context report's estimate stands in for what would otherwise
 * read 0%. Streamed usage wins once a model call has measured anything. A
 * report for the session's current model owns the window: its effective cap
 * (choco-pi's 600K) beats the raw model window the stream carries. A report
 * for another model is stale, so the streamed window wins over it.
 */
export function meterUsage(
  context: ContextUsage | null | undefined,
  report: ContextReport | null | undefined,
  sessionModel?: string | null,
): { usage: ContextUsage | null; estimated: boolean } {
  if (!report) return { usage: context ?? null, estimated: false }
  const window = reportApplies(report, sessionModel)
    ? report.contextWindow || context?.window || null
    : context?.window || report.contextWindow || null
  if (context && context.tokens > 0) return { usage: { ...context, window }, estimated: false }
  return report.measuredTokens == null
    ? { usage: { tokens: report.estimatedTokens, window }, estimated: true }
    : { usage: { tokens: report.measuredTokens, window }, estimated: false }
}

/**
 * Whether a context report describes the session's current model. An unknown
 * model on either side cannot contradict it. Mirrors the desktop's
 * `report_applies`.
 */
function reportApplies(report: ContextReport, sessionModel: string | null | undefined) {
  const reported = report.model?.trim()
  const active = sessionModel?.trim()
  return !reported || !active || reported === active
}

export type BreakdownPart =
  | { kind: 'category'; index: number }
  | { kind: 'free' }
  | { kind: 'reserve' }

export interface BreakdownSegment {
  part: BreakdownPart
  tokens: number
  /** Width as a fraction of the whole bar. */
  fraction: number
}

/**
 * The report's categories in order, then free space, then the autocompact
 * reserve. A window the categories overrun widens the bar instead of letting
 * segments spill past it.
 */
export function breakdownSegments(report: ContextReport): BreakdownSegment[] {
  const used = report.categories.reduce((sum, category) => sum + category.tokens, 0)
  const reserve = report.autocompactReserve
  const free = Math.max(0, report.contextWindow - used - reserve)
  const total = Math.max(report.contextWindow, used + reserve, 1)
  const parts: Array<[BreakdownPart, number]> = [
    ...report.categories.map((category, index): [BreakdownPart, number] => [
      { kind: 'category', index },
      category.tokens,
    ]),
    [{ kind: 'free' }, free],
    [{ kind: 'reserve' }, reserve],
  ]
  return parts
    .filter(([, tokens]) => tokens > 0)
    .map(([part, tokens]) => ({ part, tokens, fraction: tokens / total }))
}

/**
 * A window's reset as the provider phrases it when its verb is not a plain
 * "resets" (a regenerating bucket): "regenerates in 3h 20m", the way its
 * text report reads. `null` means the localized reset label applies.
 * Mirrors the desktop's `usage_window_reset`.
 */
export function providerResetPhrase(
  label: string | null | undefined,
  resetsAt: number,
  nowSeconds: number,
): string | null {
  const verb = label?.trim()
  if (!verb || verb.toLowerCase() === 'resets') return null
  const delta = resetsAt - nowSeconds
  if (delta <= 0) return `${verb} soon`
  const total = Math.ceil(delta / 60)
  const days = Math.floor(total / 1440)
  const hours = Math.floor((total % 1440) / 60)
  const minutes = days > 0 ? 0 : total % 60
  const span = [[days, 'd'], [hours, 'h'], [minutes, 'm']]
    .filter(([value]) => Number(value) > 0)
    .map(([value, unit]) => `${value}${unit}`)
    .join(' ')
  return `${verb} in ${span}`
}

/** The share of a quota window spent, whichever way the provider counts. */
export function usedPercent(window: { percent: number; qualifier: 'used' | 'remaining' }) {
  const percent = Math.min(100, Math.max(0, window.percent))
  return window.qualifier === 'remaining' ? 100 - percent : percent
}

/** Status rows a collapsed card shows, in this order, matched by label. */
const STATUS_KEY_LABELS = ['Model', 'Context usage', 'Tokens', 'Messages']
/** Rows a collapsed status card shows when none of the key labels exist. */
const STATUS_FALLBACK_ROWS = 4
/** Category swatches a collapsed context card names beside its bar. */
const CONTEXT_TOP_CATEGORIES = 3

export interface ReportSummary {
  /** Status rows shown collapsed, in display order. */
  rows: ProviderReportRow[]
  /** Indices into the context block's categories, largest first. */
  topCategories: number[]
  /** Sections the collapsed card does not show in full. */
  hiddenSections: number
}

/**
 * The collapsed form of a report card, or `null` when it has none: a usage
 * report, a context report without its typed block, or a status report whose
 * rows all fit the summary anyway. Mirrors the desktop's `report_summary`.
 */
export function reportSummary(report: ProviderReport): ReportSummary | null {
  if (report.kind === 'context') {
    const context = report.context
    if (!context) return null
    const topCategories = context.categories
      .map((category, index) => ({ index, tokens: category.tokens }))
      .filter((category) => category.tokens > 0)
      // Stable, so equal categories keep the provider's order.
      .sort((a, b) => b.tokens - a.tokens)
      .slice(0, CONTEXT_TOP_CATEGORIES)
      .map((category) => category.index)
    return {
      rows: [],
      topCategories,
      hiddenSections: report.sections.filter((section) => section.rows.length > 0).length,
    }
  }
  if (report.kind !== 'status') return null
  const allRows = report.sections.flatMap((section) => section.rows)
  let rows = STATUS_KEY_LABELS
    .map((key) => allRows.find((row) => row.label.trim().toLowerCase() === key.toLowerCase()))
    .filter((row): row is ProviderReportRow => row !== undefined)
  if (rows.length === 0) {
    rows = report.sections.find((section) => section.rows.length > 0)?.rows.slice(0, STATUS_FALLBACK_ROWS) ?? []
  }
  const shown = new Set(rows)
  const hiddenSections = report.sections
    .filter((section) => section.rows.some((row) => !shown.has(row)))
    .length
  return hiddenSections > 0 ? { rows, topCategories: [], hiddenSections } : null
}
