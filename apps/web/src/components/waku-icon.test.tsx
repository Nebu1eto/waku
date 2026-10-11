import { expect, test } from 'bun:test'
import { PROVIDERS, ProviderIcon, providerMeta } from './waku-icon'

test('choco-pi has distinct provider metadata and the desktop icon', () => {
  expect(providerMeta('chocoPi')).toEqual({
    id: 'chocoPi',
    name: 'choco-pi',
    shortName: 'choco-pi',
    command: 'pi',
  })
  expect(PROVIDERS.findIndex((provider) => provider.id === 'chocoPi'))
    .toBe(PROVIDERS.findIndex((provider) => provider.id === 'ohMyPi') + 1)
  const icon = ProviderIcon({ provider: 'chocoPi', label: 'choco-pi' })
  expect(icon.props['aria-label']).toBe('choco-pi')
  expect(icon.props.children.props.className).toBe('i-waku-provider-chocopi')
})
