import type { ContextReport, ContextUsage } from '@waku/client'

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
 * read 0%. Streamed usage wins once a model call has measured anything.
 */
export function meterUsage(
  context: ContextUsage | null | undefined,
  report: ContextReport | null | undefined,
): { usage: ContextUsage | null; estimated: boolean } {
  if (!report) return { usage: context ?? null, estimated: false }
  const window = context?.window || report.contextWindow || null
  if (context && context.tokens > 0) return { usage: { ...context, window }, estimated: false }
  return report.measuredTokens == null
    ? { usage: { tokens: report.estimatedTokens, window }, estimated: true }
    : { usage: { tokens: report.measuredTokens, window }, estimated: false }
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
