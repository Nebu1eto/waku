import { describe, expect, test } from 'bun:test'
import type { AgentSession, SequencedEvent } from '@waku/client'
import { reduceRuntimeEvent } from './event-reducer'

const clock = {
  nowSeconds: () => 200,
  nowMillis: () => 200_000,
  randomUUID: (() => {
    let id = 0
    return () => `00000000-0000-4000-8000-${String(++id).padStart(12, '0')}`
  })(),
}

describe('reduceRuntimeEvent', () => {
  test('preserves event order across reasoning, tools, and assistant text', () => {
    let session = runningSession()
    session = apply(session, 'reasoningDelta', 'Thinking')
    session = apply(session, 'richActivity', {
      id: '10000000-0000-4000-8000-000000000001',
      source_id: 'tool-1',
      kind: 'fileRead',
      title: 'Read file',
      detail: 'src/app.rs',
      failed: false,
      complete: true,
    })
    session = apply(session, 'textDelta', 'Done')
    session = apply(session, 'textDelta', '.')

    expect(session.transcript_blocks[0]?.content).toMatchObject({
      kind: 'activities',
      data: [
        { reasoning: { content: 'Thinking' }, complete: true },
        { source_id: 'tool-1', title: 'Read file' },
      ],
    })
    expect(session.messages.at(-1)?.content).toBe('Done.')
  })

  test('settles the active turn and finalizes streaming output', () => {
    let session = runningSession()
    session = apply(session, 'textDelta', 'Ready')
    const result = reduceRuntimeEvent(
      session,
      event('turnFinished', { success: true, summary: null }),
      clock,
    )

    expect(result.settled).toBe(true)
    expect(result.session.status).toBe('idle')
    expect(result.session.turns[0]?.status).toBe('completed')
    expect(result.session.messages.at(-1)?.streaming).toBe(false)
  })

  test('does not turn a completed session into a failure when its process exits', () => {
    let session = runningSession()
    session = reduceRuntimeEvent(
      session,
      event('turnFinished', { success: true, summary: null }),
      clock,
    ).session
    const result = reduceRuntimeEvent(session, event('processExited', null), clock)

    expect(result.session.status).toBe('idle')
    expect(result.session.turns[0]?.status).toBe('completed')
    expect(result.settled).toBe(false)
    expect(result.removeRuntime).toBe(true)
  })

  test('stores the daemon sequence incorporated into the transcript', () => {
    const wire = { ...event('textDelta', 'hello'), sequence: 42 }
    const result = reduceRuntimeEvent(runningSession(), wire, clock)

    expect(result.session.runtime_event_cursor).toEqual({
      runtime_id: 'runtime',
      epoch: 'epoch',
      sequence: 42,
    })
  })

  test('records Claude resume position on the active turn', () => {
    const result = reduceRuntimeEvent(
      runningSession(),
      event('connected', {
        provider: 'claude',
        sessionId: 'provider-session',
        resumeAt: 'provider-message',
      }),
      clock,
    )

    expect(result.session.turns[0]?.provider_resume_at).toBe('provider-message')
  })

  test('ignores late turn output and permission events after a turn settles', () => {
    let session = reduceRuntimeEvent(
      runningSession(),
      event('turnFinished', { success: true, summary: null }),
      clock,
    ).session
    const messageCount = session.messages.length
    session = apply(session, 'textDelta', 'late output')
    session = apply(session, 'richActivity', {
      id: 'late-tool',
      source_id: 'late-tool',
      kind: 'tool',
      title: 'Late tool',
      failed: false,
      complete: true,
    })
    const permission = reduceRuntimeEvent(
      session,
      event('permission', { requestId: 'late', title: 'Late', detail: '', options: [] }),
      clock,
    )

    expect(permission.session.messages).toHaveLength(messageCount)
    expect(permission.session.transcript_blocks).toHaveLength(0)
    expect(permission.session.status).toBe('idle')
    expect(permission.permission).toBeUndefined()
  })

  test('surfaces structured provider questions and clears them when the turn settles', () => {
    const requested = reduceRuntimeEvent(
      runningSession(),
      event('userInputRequested', {
        requestId: 'question-request',
        questions: [{
          id: 'deployment',
          header: 'Environment',
          question: 'Where should this deploy?',
          options: [{ label: 'Preview', description: 'Create a preview deployment' }],
          multiSelect: false,
        }],
      }),
      clock,
    )

    expect(requested.session.status).toBe('waiting')
    expect(requested.userInput?.questions[0]).toMatchObject({
      id: 'deployment',
      options: [{ label: 'Preview' }],
    })

    const settled = reduceRuntimeEvent(
      requested.session,
      event('turnFinished', { success: true, summary: null }),
      clock,
    )
    expect(settled.userInput).toBeNull()
  })

  test('keeps the provider error when a working runtime exits', () => {
    let session = apply(runningSession(), 'turnStarted', null)
    const errored = reduceRuntimeEvent(session, event('error', 'provider exploded'), clock)
    expect(errored.session.status).toBe('working')
    session = reduceRuntimeEvent(
      errored.session,
      event('processExited', null),
      clock,
      errored.error,
    ).session

    expect(session.status).toBe('failed')
    expect(session.messages.at(-1)?.content).toBe('provider exploded')
  })

  test('surfaces a startup error immediately without duplicating it on exit', () => {
    const errored = reduceRuntimeEvent(
      runningSession(),
      event('error', 'could not start provider'),
      clock,
    )
    expect(errored.session.status).toBe('failed')
    expect(errored.session.messages.at(-1)?.content).toBe('could not start provider')

    const exited = reduceRuntimeEvent(
      errored.session,
      event('processExited', null),
      clock,
      errored.error,
    )
    expect(exited.session.messages.filter((message) => message.role === 'assistant')).toHaveLength(1)
  })
})

function apply(session: AgentSession, kind: string, payload: unknown) {
  return reduceRuntimeEvent(session, event(kind, payload), clock).session
}

describe('provider notices', () => {
  test('a handled command turn shows its notice instead of the fallback', () => {
    let session = runningSession()
    session = apply(session, 'notice', { level: 'info', message: 'Context\n  used: 12%' })
    const notice = session.messages.at(-1)
    expect(notice).toMatchObject({ role: 'system', turn_id: 'turn', content: 'Context\n  used: 12%' })

    session = apply(session, 'turnFinished', { success: true, summary: null })
    expect(session.status).toBe('idle')
    expect(session.messages.filter((message) => message.role === 'assistant')).toHaveLength(0)
  })

  test('a failed turn still shows its failure after a notice', () => {
    let session = runningSession()
    session = apply(session, 'notice', { level: 'warning', message: 'Careful' })
    session = apply(session, 'turnFinished', { success: false, summary: 'boom' })
    expect(session.messages.at(-1)).toMatchObject({ role: 'assistant', content: 'boom' })
  })

  test('a notice between turns stays outside them, and empty ones are ignored', () => {
    let session: AgentSession = { ...runningSession(), status: 'idle', turns: [] }
    session = apply(session, 'notice', { level: 'info', message: '  ' })
    expect(session.messages).toHaveLength(1)
    session = apply(session, 'notice', { level: 'info', message: 'Subagent @a finished' })
    expect(session.messages.at(-1)).toMatchObject({ role: 'system', turn_id: null })
  })
})

// Hand-written to choco-pi's published report shape.
const contextReport = {
  schema: 1,
  kind: 'context',
  at: '2026-10-11T08:00:00.000Z',
  sessionId: 's1',
  title: 'Context Usage',
  sections: [{ title: 'Totals', rows: [{ label: 'Used', value: '\u224812k', hint: 'estimated' }] }],
  context: {
    model: 'anthropic/claude-opus',
    contextWindow: 200_000,
    measuredTokens: null,
    estimatedTokens: 12_000,
    autocompactReserve: 16_000,
    categories: [{ label: 'System prompt', tokens: 12_000, estimated: true }],
    tools: { active: 3, deferred: 9 },
    mcp: { servers: [] },
    skills: { count: 1, tokens: 10 },
    contextFiles: { count: 0, tokens: 0 },
  },
}

describe('provider reports', () => {
  test('an automatic context report updates the live breakdown without a card', () => {
    let session: AgentSession = { ...runningSession(), status: 'idle', turns: [] }
    session = apply(session, 'report', { report: contextReport, invoked: false, notice: null })
    expect(session.latest_context_report).toMatchObject({ title: 'Context Usage' })
    expect(session.latest_context_report?.context?.measuredTokens).toBeNull()
    expect(session.messages).toHaveLength(1)
  })

  test('an invoked report is its command turn\'s card reply', () => {
    let session = runningSession()
    session = apply(session, 'report', {
      report: contextReport,
      invoked: true,
      notice: 'Context Usage\n  used: 6%',
    })
    expect(session.messages.at(-1)).toMatchObject({
      role: 'system',
      turn_id: 'turn',
      content: 'Context Usage\n  used: 6%',
      report: { kind: 'context', title: 'Context Usage' },
    })
    expect(session.latest_context_report?.title).toBe('Context Usage')

    session = apply(session, 'turnFinished', { success: true, summary: null })
    expect(session.messages.filter((message) => message.role === 'assistant')).toHaveLength(0)
  })

  test('a status report never replaces the context breakdown', () => {
    let session = apply(runningSession(), 'report', { report: contextReport, invoked: false })
    session = apply(session, 'report', {
      report: { schema: 1, kind: 'status', at: 'now', title: 'Status', sections: [] },
      invoked: true,
      notice: null,
    })
    expect(session.latest_context_report?.kind).toBe('context')
    expect(session.messages.at(-1)).toMatchObject({ content: 'Status', report: { kind: 'status' } })
  })

  test('malformed reports are dropped', () => {
    const before = runningSession()
    for (const report of [
      null,
      { ...contextReport, schema: 2 },
      { ...contextReport, kind: 'usage' },
      { ...contextReport, title: ' ' },
      { ...contextReport, sections: [{ title: 'x', rows: [{ label: 'a' }] }] },
      { ...contextReport, context: { ...contextReport.context, contextWindow: '200k' } },
    ]) {
      const session = apply(before, 'report', { report, invoked: true, notice: 'x' })
      expect(session.messages).toHaveLength(1)
      expect(session.latest_context_report).toBeUndefined()
    }
  })
})

describe('thread goals', () => {
  test('stores goal updates and names a goal-first task from its objective', () => {
    let session: AgentSession = { ...runningSession(), status: 'idle', messages: [], turns: [] }
    session = apply(session, 'goalUpdated', {
      objective: 'Improve benchmark coverage across the suite',
      status: 'active',
      tokenBudget: null,
      tokensUsed: 0,
      timeUsedSeconds: 0,
    })

    expect(session.thread_goal?.objective).toBe('Improve benchmark coverage across the suite')
    expect(session.thread_goal?.status).toBe('active')
    expect(session.auto_title).toBe('Improve benchmark coverage across the suite')

    session = apply(session, 'goalUpdated', null)
    expect(session.thread_goal).toBeNull()
  })

  test('an unsolicited codex turn gets a transcript home and streams output', () => {
    let session: AgentSession = { ...runningSession(), status: 'idle', messages: [], turns: [] }
    session = apply(session, 'turnStarted', null)

    expect(session.status).toBe('working')
    expect(session.turns).toHaveLength(1)
    expect(session.turns[0]?.provider_turn_started).toBe(true)

    session = apply(session, 'textDelta', 'Hi!')
    expect(session.messages.at(-1)).toMatchObject({
      role: 'assistant',
      content: 'Hi!',
      turn_id: session.turns[0]?.id,
    })

    const settled = reduceRuntimeEvent(
      session,
      event('turnFinished', { success: true, summary: null }),
      clock,
    )
    expect(settled.settled).toBe(true)
    expect(settled.session.status).toBe('idle')
  })

  test('a parked claude turn stays open and its wake continues it', () => {
    let session: AgentSession = { ...runningSession(), provider: 'claude' }
    session = apply(session, 'textDelta', 'Started it in the background.')
    session = apply(session, 'turnParked', null)
    expect(session.status).toBe('background')
    expect(session.turns).toHaveLength(1)
    expect(session.turns[0]?.status).toBe('running')
    expect(session.messages.at(-1)?.streaming).toBe(false)

    session = apply(session, 'turnStarted', null)
    expect(session.status).toBe('working')
    expect(session.turns).toHaveLength(1)
    expect(session.turns[0]?.id).toBe('turn')

    const settled = reduceRuntimeEvent(
      session,
      event('turnFinished', { success: true, summary: null }),
      clock,
    )
    expect(settled.settled).toBe(true)
    expect(settled.session.status).toBe('idle')
    expect(settled.session.turns).toHaveLength(1)
  })

  test('a claude wake with no held turn gets a transcript home', () => {
    const idle: AgentSession = {
      ...runningSession(),
      provider: 'claude',
      status: 'idle',
      messages: [],
      turns: [],
    }
    const woken = apply(idle, 'turnStarted', null)
    expect(woken.status).toBe('working')
    expect(woken.turns).toHaveLength(1)
    expect(woken.turns[0]?.provider_turn_started).toBe(true)
  })

  test('choco-pi background completions get a new transcript turn without a user message', () => {
    const idle: AgentSession = {
      ...runningSession(),
      provider: 'chocoPi',
      status: 'idle',
      messages: [],
      turns: [],
    }
    const woken = apply(idle, 'turnStarted', null)
    const streaming = apply(woken, 'textDelta', 'Background work finished')
    expect(streaming.status).toBe('working')
    expect(streaming.turns).toHaveLength(1)
    expect(streaming.turns[0]?.provider_turn_started).toBe(true)
    expect(streaming.messages).toHaveLength(1)
    expect(streaming.messages[0]?.role).toBe('assistant')
    expect(streaming.messages[0]?.content).toBe('Background work finished')
    const settled = apply(streaming, 'turnFinished', { success: true, summary: null })
    expect(settled.status).toBe('idle')
    expect(settled.turns).toHaveLength(1)
    const next = apply(settled, 'turnStarted', null)
    expect(next.turns).toHaveLength(2)
    expect(apply(next, 'turnStarted', null).turns).toHaveLength(2)

    expect(apply({ ...idle, provider: 'pi' }, 'turnStarted', null).turns).toHaveLength(0)
    expect(apply({ ...idle, provider: 'ohMyPi' }, 'turnStarted', null).turns).toHaveLength(0)
  })

  test('unsolicited turns belong to self-waking providers and never preempt an active turn', () => {
    const amp: AgentSession = {
      ...runningSession(),
      provider: 'amp',
      status: 'idle',
      messages: [],
      turns: [],
    }
    expect(apply(amp, 'turnStarted', null).turns).toHaveLength(0)

    const active = runningSession()
    const next = apply(active, 'turnStarted', null)
    expect(next.turns).toHaveLength(1)
    expect(next.turns[0]?.id).toBe('turn')
  })
})

function event(kind: string, payload: unknown): SequencedEvent {
  return {
    sessionId: 'session',
    runtimeId: 'runtime',
    epoch: 'epoch',
    sequence: 1,
    event: { kind, payload: payload as never },
  }
}

function runningSession(): AgentSession {
  return {
    id: 'session',
    title: 'New task',
    project_id: 'project',
    workspace: { kind: 'local' },
    provider: 'codex',
    runtime_mode: 'fullAccess',
    status: 'connecting',
    created_at: 100,
    updated_at: 100,
    provider_cursor: null,
    messages: [
      {
        id: 'message',
        turn_id: 'turn',
        role: 'user',
        content: 'Go',
        created_at: 100,
        streaming: false,
      },
    ],
    transcript_blocks: [],
    turns: [
      {
        id: 'turn',
        turn_count: 1,
        status: 'running',
        provider_turn_started: false,
        started_at: 100,
        completed_at: null,
        checkpoint: null,
      },
    ],
  }
}
