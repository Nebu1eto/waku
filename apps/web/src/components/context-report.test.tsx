import { expect, test } from 'bun:test'
import type { ProviderReport } from '@waku/client'
import { isValidElement, type ReactNode } from 'react'
import { renderToStaticMarkup } from 'react-dom/server'
import { translate } from '@/lib/i18n'
import { ReportCard, ReportCardView } from './context-report'

const t = (key: string, params?: Record<string, string | number>) => translate('en', key, params)

async function fixture(kind: 'status' | 'context'): Promise<ProviderReport> {
  const url = new URL(`../../../../crates/waku-protocol/src/fixtures/choco_pi_report_${kind}.json`, import.meta.url)
  return JSON.parse(await Bun.file(url).text())
}

/** The first intrinsic element in `node` that carries `aria-expanded`. */
function findToggle(node: ReactNode): { onClick: () => void } | null {
  if (Array.isArray(node)) {
    for (const child of node) {
      const found = findToggle(child)
      if (found) return found
    }
    return null
  }
  if (!isValidElement<{ children?: ReactNode; onClick?: () => void; 'aria-expanded'?: boolean }>(node)) return null
  const { props } = node
  if (props['aria-expanded'] !== undefined && props.onClick) return { onClick: props.onClick }
  return findToggle(props.children)
}

test('a status card starts collapsed to its key rows behind a real toggle', async () => {
  const markup = renderToStaticMarkup(<ReportCard report={await fixture('status')} t={t} />)
  expect(markup).toContain('Claude Fable 5.1 (claude-fable-5-1)')
  expect(markup).toContain('Context usage')
  expect(markup).not.toContain('Pi version')
  expect(markup).toMatch(/<button aria-expanded="false"[^>]*type="button"/)
  expect(markup).toContain('Show details')
  expect(markup).toContain('2 sections')
})

test('the toggle expands the card and flips aria-expanded', async () => {
  const report = await fixture('status')
  let expanded = false
  const view = () => ReportCardView({ expanded, onToggle: () => { expanded = !expanded }, report, t })
  const toggle = findToggle(view())
  expect(toggle).not.toBeNull()
  toggle?.onClick()
  expect(expanded).toBe(true)
  const markup = renderToStaticMarkup(view())
  expect(markup).toMatch(/<button aria-expanded="true"/)
  expect(markup).toContain('Hide details')
  expect(markup).toContain('Pi version')
  expect(markup).toContain('Session Info')
  findToggle(view())?.onClick()
  expect(expanded).toBe(false)
})

test('a collapsed context card keeps the totals and bar but not the per-item rows', async () => {
  const report = await fixture('context')
  const collapsed = renderToStaticMarkup(<ReportCard report={report} t={t} />)
  expect(collapsed).toContain('\u22486.9k / 600.0k (\u22481%)')
  expect(collapsed).toContain('System tools')
  expect(collapsed).not.toContain('Inventory')
  expect(collapsed).not.toContain('Free space')
  expect(collapsed).toMatch(/<button aria-expanded="false"/)
  const expanded = renderToStaticMarkup(ReportCardView({ expanded: true, onToggle: () => {}, report, t }))
  expect(expanded).toContain('Inventory')
  expect(expanded).toContain('Free space')
})

test('a usage card has no disclosure', () => {
  const report: ProviderReport = {
    schema: 1,
    kind: 'usage',
    at: '2026-10-11T09:27:51.649Z',
    title: 'Usage',
    sections: [{ title: 'Plan', rows: [{ label: 'Claude', value: '12% used' }] }],
  }
  expect(renderToStaticMarkup(<ReportCard report={report} t={t} />)).not.toContain('aria-expanded')
})
