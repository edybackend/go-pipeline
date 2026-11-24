// PreToolUse hook: concurrency rules for internal/ (tests excluded). Each one prevents a classic pipeline bug.
//   - No root contexts: every stage must stop when the caller cancels, so the context is passed in, never created.
//   - No time.Sleep: a sleeping goroutine ignores cancellation; wait with select on ctx.Done() and a timer.
//   - No huge channel buffers: a big buffer hides a slow stage and turns backpressure into memory growth.
import { readFileSync } from 'node:fs'

const input = JSON.parse(readFileSync(0, 'utf8'))
const filePath = (input.tool_input?.file_path ?? '').replaceAll('\\', '/')
if (!/\/internal\/.*\.go$/.test(filePath) || filePath.endsWith('_test.go')) process.exit(0)

const written = input.tool_input?.new_string ?? input.tool_input?.content ?? ''
const rules = [
  [/context\.(Background|TODO)\(\)/, 'creates a root context; accept ctx from the caller so cancellation reaches this code'],
  [/time\.Sleep\(/, 'uses time.Sleep, which ignores cancellation; use select { case <-time.After(d): case <-ctx.Done(): }'],
  [/make\(chan [^,)]+,\s*[0-9_]{5,}\)/, 'creates a channel with a huge buffer; size channels from Config so backpressure works'],
]
const hit = rules.find(([pattern]) => pattern.test(written))
if (hit) {
  console.error(`Blocked: ${filePath} ${hit[1]}.`)
  process.exit(2)
}
