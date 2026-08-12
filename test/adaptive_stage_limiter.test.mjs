import assert from "node:assert/strict";
import test from "node:test";
import {
  createAdaptiveStageLimiter,
  parseMemAvailable,
  parsePsiAvg10,
  pipelineStageWeight,
  systemHeadroom,
  waitForSystemHeadroom
} from "../scripts/lib/adaptive_stage_limiter.mjs";

function deferred() {
  let resolve;
  const promise = new Promise(done => { resolve = done; });
  return { promise, resolve };
}

test("parses Linux available memory and pressure", () => {
  assert.equal(parseMemAvailable("MemTotal: 100 kB\nMemAvailable: 42 kB\n"), 42 * 1024);
  assert.equal(parsePsiAvg10("some avg10=1.25 avg60=0.5 avg300=0.1 total=9\n"), 1.25);
  const files = {
    "/proc/meminfo": "MemAvailable: 4194304 kB\n",
    "/proc/pressure/memory": "some avg10=3.00 avg60=0.5 avg300=0.1 total=9\n"
  };
  assert.equal(systemHeadroom({
    readFile: path => files[path],
    minAvailableBytes: 2 * 1024 ** 3,
    maxMemoryPsiAvg10: 2
  }).healthy, false);
});

test("weights large road work more heavily than place extraction", () => {
  const common = { sourceBytes: 2_000, largePbfBytes: 1_000, capacity: 4 };
  assert.equal(pipelineStageWeight({ ...common, stage: "places" }), 2);
  assert.equal(pipelineStageWeight({ ...common, stage: "roads" }), 3);
  assert.equal(pipelineStageWeight({ ...common, stage: "roads", sourceBytes: 10 }), 2);
});

test("weights enrichment by its address partition instead of only PBF size", () => {
  const common = { stage: "enrichment", sourceBytes: 10, largePbfBytes: 1_000, capacity: 4 };
  assert.equal(pipelineStageWeight({ ...common, addressBytes: 100, addressRecords: 100 }), 1);
  assert.equal(pipelineStageWeight({ ...common, addressBytes: 2_000, addressRecords: 100 }), 2);
  assert.equal(pipelineStageWeight({ ...common, addressBytes: 100, addressRecords: 5_000_000 }), 2);
});

test("runs real stages concurrently within weighted capacity", async () => {
  const limiter = createAdaptiveStageLimiter({ capacity: 4 });
  const first = deferred();
  const second = deferred();
  const events = [];
  const a = limiter.run(3, async () => { events.push("roads:start"); await first.promise; });
  const b = limiter.run(1, async () => { events.push("places:start"); await second.promise; });
  const c = limiter.run(1, async () => { events.push("next:start"); });
  await new Promise(resolve => setImmediate(resolve));
  assert.deepEqual(events, ["roads:start", "places:start"]);
  second.resolve();
  await new Promise(resolve => setImmediate(resolve));
  assert.equal(events.includes("next:start"), true);
  first.resolve();
  await Promise.all([a, b, c]);
});

test("uses a free lane behind a temporarily blocked heavy stage", async () => {
  const limiter = createAdaptiveStageLimiter({ capacity: 4 });
  const active = deferred();
  const events = [];
  const a = limiter.run(2, async () => { events.push("active"); await active.promise; });
  const blocked = limiter.run(3, async () => { events.push("heavy"); });
  const fitting = limiter.run(1, async () => { events.push("fitting"); });
  await new Promise(resolve => setImmediate(resolve));
  assert.deepEqual(events, ["active", "fitting"]);
  active.resolve();
  await Promise.all([a, blocked, fitting]);
  assert.deepEqual(events, ["active", "fitting", "heavy"]);
});

test("reports bounded active weight and elapsed stage telemetry", async () => {
  const events = [];
  const limiter = createAdaptiveStageLimiter({ capacity: 3, onChange: event => events.push(event) });
  await Promise.all([
    limiter.run(2, async () => {}, { region: "a", stage: "roads" }),
    limiter.run(1, async () => {}, { region: "b", stage: "enrichment" })
  ]);
  assert.equal(events.filter(event => event.type === "start").length, 2);
  assert.equal(events.every(event => event.used >= 0 && event.used <= event.capacity), true);
  assert.equal(events.some(event => event.type === "finish" && Number.isFinite(event.elapsedMs)), true);
});

test("telemetry failures cannot wedge or fail scheduled work", async () => {
  const limiter = createAdaptiveStageLimiter({
    capacity: 1,
    onChange() { throw new Error("telemetry unavailable"); }
  });
  let ran = false;
  await limiter.run(1, async () => { ran = true; });
  await new Promise(resolve => setImmediate(resolve));
  assert.equal(ran, true);
  assert.equal(limiter.used, 0);
});

test("rejects one stage when its pressure probe fails and continues", async () => {
  let checks = 0;
  const limiter = createAdaptiveStageLimiter({
    capacity: 1,
    beforeStart: () => {
      if (++checks === 1) throw new Error("pressure probe failed");
    }
  });
  await assert.rejects(limiter.run(1, async () => {}), /pressure probe failed/u);
  let ran = false;
  await limiter.run(1, async () => { ran = true; });
  assert.equal(ran, true);
});

test("pressure gate pauses new stages until headroom recovers", async () => {
  let samples = 0;
  let reported = 0;
  const state = await waitForSystemHeadroom({
    pollMs: 10,
    sample: async () => ({ healthy: ++samples >= 3 }),
    onPressure: () => { reported++; }
  });
  assert.equal(state.healthy, true);
  assert.equal(samples, 3);
  assert.equal(reported, 1);
});
