import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { createServer } from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { once } from "node:events";
import { test } from "node:test";

const delay = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

test("a host budget expiry resumes the same resident workflow without repeating a committed step", {
  skip: !process.env.CELLD_BIN,
  timeout: 60_000,
}, async (t) => {
  const project = await mkdtemp(join(tmpdir(), "celld-workflow-budget-"));
  await writeFile(join(project, "wrangler.jsonc"), JSON.stringify({
    name: "budget-regression", main: "index.js", compatibility_date: "2026-08-18",
    workflows: [{ binding: "RUN", name: "budget-regression", class_name: "Recovery" }],
  }));
  await writeFile(join(project, "index.js"), `
import { WorkflowEntrypoint } from "cloudflare:workers";
let committed = 0;
let interrupted = 0;
export class Recovery extends WorkflowEntrypoint {
  async run(_event, step) {
    await step.do("committed", async () => ++committed);
    const attempt = await step.do("interrupted", { timeout: 60000 }, async () => {
      const current = ++interrupted;
      if (current === 1) await new Promise(resolve => setTimeout(resolve, 10000));
      return current;
    });
    return { committed, attempt };
  }
}
export default {
  async fetch(request, env) {
    const path = new URL(request.url).pathname;
    if (path === "/create") return Response.json(await env.RUN.create({ id: "regression", params: {} }));
    if (path === "/status") return Response.json(await (await env.RUN.get("regression")).status());
    return new Response("ready");
  }
};
`);
  const server = createServer();
  server.listen(0, "127.0.0.1");
  await once(server, "listening");
  const port = server.address().port;
  await new Promise((resolve) => server.close(resolve));
  const child = spawn(process.env.CELLD_BIN, ["dev", project, "--host", "127.0.0.1", "--port", String(port), "--logs"], {
    env: { ...process.env, CELLD_HANDLER_BUDGET_S: "1", NO_COLOR: "1" },
    stdio: ["ignore", "pipe", "pipe"],
  });
  let logs = "";
  child.stdout.on("data", (chunk) => { logs = (logs + chunk).slice(-20_000); });
  child.stderr.on("data", (chunk) => { logs = (logs + chunk).slice(-20_000); });
  t.after(async () => {
    if (child.exitCode === null) {
      const exited = once(child, "exit");
      child.kill("SIGTERM");
      const kill = setTimeout(() => child.kill("SIGKILL"), 5000);
      await exited;
      clearTimeout(kill);
    }
    await rm(project, { recursive: true, force: true });
  });
  const url = `http://127.0.0.1:${port}`;
  async function request(path) {
    return fetch(url + path, { signal: AbortSignal.timeout(2000) });
  }
  let ready = false;
  for (let n = 0; n < 100 && child.exitCode === null; n++) {
    try { if ((await request("/")).ok) { ready = true; break; } } catch {}
    await delay(100);
  }
  assert.ok(ready, `celld did not start:\n${logs}`);
  assert.equal((await request("/create")).status, 200);
  let status;
  for (let n = 0; n < 100 && child.exitCode === null; n++) {
    try {
      const response = await request("/status");
      if (response.ok) status = await response.json();
      if (status?.status === "complete" || status?.status === "errored") break;
    } catch {}
    await delay(100);
  }
  assert.equal(status?.status, "complete", `workflow did not recover: ${JSON.stringify(status)}\n${logs}`);
  assert.deepEqual(status.output, { committed: 1, attempt: 2 });
});
