import { describe, expect, test } from 'bun:test'
import type { ContextReport, ProviderReport } from '@waku/client'
import { breakdownSegments, meterUsage, providerResetPhrase, reportSummary, usedPercent } from './context-report'

describe('usage report windows', () => {
  test('remaining windows read as used, clamped', () => {
    expect(usedPercent({ percent: 70, qualifier: 'remaining' })).toBe(30)
    expect(usedPercent({ percent: 42, qualifier: 'used' })).toBe(42)
    expect(usedPercent({ percent: 140, qualifier: 'used' })).toBe(100)
  })

  test('a provider verb other than "resets" keeps its phrase with a countdown', () => {
    const now = 1_000_000
    expect(providerResetPhrase('regenerates', now + 3 * 3600 + 20 * 60, now)).toBe('regenerates in 3h 20m')
    expect(providerResetPhrase('regenerates', now + 2 * 86_400 + 3605, now)).toBe('regenerates in 2d 1h')
    expect(providerResetPhrase('regenerates', now - 5, now)).toBe('regenerates soon')
    expect(providerResetPhrase('resets', now + 600, now)).toBeNull()
    expect(providerResetPhrase(null, now + 600, now)).toBeNull()
  })
})

function report(measuredTokens: number | null): ContextReport {
  return {
    contextWindow: 200_000,
    measuredTokens,
    estimatedTokens: 30_000,
    autocompactReserve: 20_000,
    categories: [
      { label: 'System', tokens: 10_000, estimated: true },
      { label: 'Messages', tokens: 0, estimated: false },
      { label: 'Tools', tokens: 20_000, estimated: true },
    ],
  }
}

describe('context report meter', () => {
  test('falls back to the estimate until something is measured', () => {
    expect(meterUsage(null, report(null))).toEqual({
      usage: { tokens: 30_000, window: 200_000 },
      estimated: true,
    })
    expect(meterUsage({ tokens: 0, window: null }, report(null)).estimated).toBe(true)
    expect(meterUsage(null, report(50_000))).toEqual({
      usage: { tokens: 50_000, window: 200_000 },
      estimated: false,
    })
    expect(meterUsage({ tokens: 60_000, window: null }, report(null))).toEqual({
      usage: { tokens: 60_000, window: 200_000 },
      estimated: false,
    })
    expect(meterUsage({ tokens: 5, window: 10 }, null)).toEqual({
      usage: { tokens: 5, window: 10 },
      estimated: false,
    })
  })

  test('breakdown ends with free space then the reserve', () => {
    const segments = breakdownSegments(report(null))
    expect(segments.map((segment) => [segment.part.kind, segment.tokens])).toEqual([
      ['category', 10_000],
      ['category', 20_000],
      ['free', 150_000],
      ['reserve', 20_000],
    ])
    expect(segments.reduce((sum, segment) => sum + segment.fraction, 0)).toBeCloseTo(1)
    const overrun = breakdownSegments({ ...report(null), contextWindow: 25_000 })
    expect(overrun.some((segment) => segment.part.kind === 'free')).toBe(false)
    expect(overrun.reduce((sum, segment) => sum + segment.fraction, 0)).toBeCloseTo(1)
  })
})

async function fixture(kind: 'status' | 'context' | 'usage'): Promise<ProviderReport> {
  const url = new URL(`../../../../crates/waku-protocol/src/fixtures/choco_pi_report_${kind}.json`, import.meta.url)
  return JSON.parse(await Bun.file(url).text())
}

describe('report card summary', () => {
  test('collapsed status shows its key rows across sections', async () => {
    const summary = reportSummary(await fixture('status'))
    expect(summary?.rows.map((row) => row.label)).toEqual(['Model', 'Context usage', 'Tokens', 'Messages'])
    expect(summary?.rows[0].value).toBe('Claude Fable 5.1 (claude-fable-5-1)')
    expect(summary?.hiddenSections).toBe(2)
    expect(summary?.topCategories).toEqual([])
  })

  test('status without key labels falls back to its first section', async () => {
    const report = await fixture('status')
    for (const section of report.sections) {
      for (const row of section.rows) row.label = `x-${row.label}`
    }
    expect(reportSummary(report)?.rows.map((row) => row.label)).toEqual(['x-File', 'x-ID', 'x-Messages', 'x-Tokens'])
    report.sections = [{ ...report.sections[0], rows: report.sections[0].rows.slice(0, 4) }]
    expect(reportSummary(report)).toBeNull()
  })

  test('collapsed context keeps the bar and its largest categories', async () => {
    const report = await fixture('context')
    const summary = reportSummary(report)
    expect(summary?.topCategories.map((index) => report.context?.categories[index].label))
      .toEqual(['System tools', 'Skills', 'System prompt'])
    expect(summary?.rows).toEqual([])
    expect(summary?.hiddenSections).toBe(2)
  })

  test('usage and untyped context reports never collapse', async () => {
    expect(reportSummary(await fixture('usage'))).toBeNull()
    expect(reportSummary({ ...(await fixture('context')), context: null })).toBeNull()
  })
})
