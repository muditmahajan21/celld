import assert from "node:assert/strict";
import { AsyncLocalStorage } from "node:async_hooks";
import { readFile } from "node:fs/promises";
import { test } from "node:test";
import { serialize } from "node:v8";
import vm from "node:vm";

const harness = await readFile(process.env.CELLD_HARNESS_PATH ?? new URL("../crates/celld/js/harness.js", import.meta.url), "utf8");
const workflowSource = harness.slice(
  harness.indexOf("const __WF_VALUE_CAP"),
  harness.indexOf("class WorkflowInstance"),
);
const retirementSource = harness.slice(
  harness.indexOf("__celld.__retireInputGateContext ="),
  harness.indexOf("// Race a pending dispatch against the caller's abort signal"),
);

// Run the production workflow driver. Only its host storage, clock and export
// bindings are supplied here; none of the step or recovery logic is duplicated.
function world(run) {
  const values = new Map();
  const alarms = new Map();
  const overflowedTimers = [];
  const local = new AsyncLocalStorage();
  const copy = (value) => value === undefined ? undefined : structuredClone(value);
  const kv = {
    get: (key) => copy(values.get(key)),
    put: (key, value) => values.set(key, copy(value)),
    delete: (key) => values.delete(key),
    list: ({ prefix = "" } = {}) => new Map(
      [...values].filter(([key]) => key.startsWith(prefix)).sort().map(([key, value]) => [key, copy(value)]),
    ),
  };
  const storage = {
    _scope: "fixture",
    kv,
    get: async (key) => kv.get(key),
    put: async (key, value) => kv.put(key, value),
    getAlarm: async () => alarms.get("fixture") ?? null,
    setAlarm: async (at) => alarms.set("fixture", at),
    deleteAlarm: async () => alarms.delete("fixture"),
    transactionSync: (work) => work(),
  };
  let WorkflowCell;
  const internals = {};
  const context = vm.createContext({
    console, clearTimeout, Date, crypto: globalThis.crypto,
    setTimeout: (fn, ms, ...args) => {
      // Match the production timer's signed 32-bit host boundary.
      if (ms > 0x7fffffff) overflowedTimers.push(ms);
      return setTimeout(fn, Math.max(0, ms | 0), ...args);
    },
    __cell: { workflows: { fixture: "Fixture" }, env: {} },
    __cf: { exports: { Fixture: class { run(event, step) { return run(event, step); } } } },
    __celld: internals,
    __runtimeClass: (_name, cls) => { WorkflowCell = cls; },
    __sc_encode: serialize,
    __io_context_id: () => "fixture-event",
    __als_get: () => local.getStore(),
    __als_set: (frame) => local.enterWith(frame),
    __alarm_set: (scope, at) => alarms.set(scope, at),
    __alarm_delete: (scope) => alarms.delete(scope),
    __registerWaitUntil: () => {},
    __cellBlocks: new Map(),
    __endActorEventsForContext: () => {},
  });
  vm.runInContext(workflowSource + retirementSource, context, { filename: "harness-workflows.js" });
  const cell = new WorkflowCell({ storage });
  return {
    cell, internals, values, overflowedTimers,
    async create() {
      await cell.__wfCreate({ workflowName: "fixture", instanceId: "instance", params: {} });
    },
    async fire() { return cell.alarm(); },
    status: () => cell.__wfStatus(),
  };
}

const delay = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

test("a retired invocation replays on the same resident instance and fences its late result", { timeout: 300 }, async () => {
  let committedCalls = 0;
  let interruptedCalls = 0;
  let releaseOld;
  let enteredOld;
  const entered = new Promise((resolve) => { enteredOld = resolve; });
  const w = world(async (_event, step) => {
    await step.do("committed", async () => ++committedCalls);
    return step.do("interrupted", { timeout: 500 }, async () => {
      if (++interruptedCalls !== 1) return "fresh";
      enteredOld();
      return new Promise((resolve) => { releaseOld = resolve; });
    });
  });
  await w.create();
  const first = w.fire();
  await entered;
  w.internals.__retireInputGateContext("fixture-event");
  await first;
  assert.equal((await w.status()).status, "running", "retirement must leave the durable run resumable");
  await w.fire();
  assert.deepEqual(JSON.parse(JSON.stringify(await w.status())), {
    status: "complete", rollback: null, output: "fresh",
  });
  assert.equal(committedCalls, 1, "a committed step must be replayed from its journal");
  assert.equal(interruptedCalls, 2);
  releaseOld("stale");
  await delay(0);
  assert.equal((await w.status()).output, "fresh");
  const interrupted = [...w.values].find(([key]) => key.endsWith(".interrupted"))?.[1];
  assert.equal(interrupted.value, "fresh", "the abandoned callback must not overwrite the new ledger");
});

test("a due retry progresses while a sibling keeps starting successful steps", async () => {
  const events = [];
  const w = world(async (_event, step) => {
    const retry = step.do("interrupted-read", {
      retries: { limit: 1, delay: 15, backoff: "constant" }, timeout: 500,
    }, async ({ attempt }) => {
      if (attempt === 1) throw new Error("temporarily unavailable");
      events.push("retried");
      return "read";
    });
    const sibling = (async () => {
      for (let i = 0; i < 8; i++) {
        await step.do(`sibling-${i}`, { timeout: 500 }, async () => {
          await delay(8);
          events.push(`sibling-${i}`);
          return i;
        });
      }
    })();
    await Promise.all([retry, sibling]);
    return "complete";
  });
  await w.create();
  await w.fire();
  assert.notEqual(events.indexOf("retried"), -1, "the due retry must run during the active drive");
  assert.ok(events.indexOf("retried") < events.indexOf("sibling-7"));
  assert.equal((await w.status()).status, "complete");
});

test("an idle retry stays durable and runs only when its alarm replays", async () => {
  let calls = 0;
  const w = world(async (_event, step) => step.do("read", {
    retries: { limit: 1, delay: 15, backoff: "constant" }, timeout: 500,
  }, async () => {
    if (++calls === 1) throw new Error("temporarily unavailable");
    return "read";
  }));
  await w.create();
  await w.fire();
  assert.equal((await w.status()).status, "waiting");
  await delay(25);
  assert.equal(calls, 1, "a suspended drive must not execute its old retry timer");
  await w.fire();
  assert.equal((await w.status()).status, "complete");
  assert.equal(calls, 2);
});

test("a permanent failure beside an active sibling is not retried", async () => {
  let calls = 0;
  const w = world(async (_event, step) => Promise.allSettled([
    step.do("refused", { retries: { limit: 0, delay: 1 } }, async () => {
      calls++;
      throw new Error("refused");
    }),
    step.do("sibling", async () => { await delay(20); return "read"; }),
  ]).then((results) => results.map(({ status }) => status)));
  await w.create();
  await w.fire();
  assert.equal(calls, 1);
  assert.deepEqual(JSON.parse(JSON.stringify((await w.status()).output)), ["rejected", "fulfilled"]);
});

test("a long retry backoff does not wrap into immediate timer/storage work", async () => {
  let calls = 0;
  const w = world(async (_event, step) => Promise.all([
    step.do("read", { retries: { limit: 1, delay: "30 days" } }, async () => {
      calls++;
      throw new Error("temporarily unavailable");
    }),
    step.do("sibling", async () => { await delay(20); return "read"; }),
  ]));
  await w.create();
  await w.fire();
  assert.equal((await w.status()).status, "waiting");
  assert.equal(calls, 1);
  assert.deepEqual(w.overflowedTimers, [], "a future backoff must stay in the future at the host timer boundary");
});
