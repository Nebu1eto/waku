import type { DaySlice, ProviderDay, UsageProvider } from '@waku/client'

export interface UsageProviderStyle {
  id: UsageProvider
  label: string
  /** Series color for charts, as a CSS color value. */
  color: string
  /** Mark color, as a literal class so Tailwind sees it. */
  iconClass: string
  /** Bar-segment fill, as a literal class so Tailwind sees it. */
  barClass: string
}

/**
 * Usage providers in `byProvider` order (the daemon's `UsageProvider::ALL`).
 * choco-pi's brand mark is the same neutral as Codex's, so it gets its own
 * Usage-only hue to keep the series apart.
 */
export const USAGE_PROVIDERS = [
  { id: 'claude', label: 'Claude Code', color: '#d97757', iconClass: 'text-[#d97757]', barClass: 'bg-[#d97757]' },
  { id: 'codex', label: 'Codex', color: 'var(--provider-codex)', iconClass: 'text-foreground', barClass: 'bg-[var(--provider-codex)]' },
  { id: 'chocoPi', label: 'choco-pi', color: 'var(--provider-chocopi)', iconClass: 'text-[var(--provider-chocopi)]', barClass: 'bg-[var(--provider-chocopi)]' },
] as const satisfies readonly UsageProviderStyle[]

const STYLES: Record<UsageProvider, UsageProviderStyle> = {
  claude: USAGE_PROVIDERS[0],
  codex: USAGE_PROVIDERS[1],
  chocoPi: USAGE_PROVIDERS[2],
}

export function usageProviderStyle(provider: UsageProvider): UsageProviderStyle {
  return STYLES[provider]
}

/** Pairs each provider's style with its `byProvider` entry, in order. */
export function usageProviderEntries(byProvider: DaySlice['byProvider']): Array<[UsageProviderStyle, ProviderDay]> {
  return [
    [USAGE_PROVIDERS[0], byProvider[0]],
    [USAGE_PROVIDERS[1], byProvider[1]],
    [USAGE_PROVIDERS[2], byProvider[2]],
  ]
}
