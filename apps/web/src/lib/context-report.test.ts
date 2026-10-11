import { describe, expect, test } from 'bun:test'
import type { ContextReport } from '@waku/client'
import { breakdownSegments, meterUsage, providerResetPhrase, usedPercent } from './context-report'

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
