import { readFileSync } from "node:fs";
import { setTimeout as delay } from "node:timers/promises";

function number(value, fallback) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : fallback;
}

export function parseMemAvailable(text) {
  const match = String(text || "").match(/^MemAvailable:\s+(\d+)\s+kB$/mu);
  return match ? Number(match[1]) * 1024 : null;
}

export function parsePsiAvg10(text) {
  const match = String(text || "").match(/^some\s+[^\n]*\bavg10=(\d+(?:\.\d+)?)/mu);
  return match ? Number(match[1]) : null;
}

export function systemHeadroom({
  readFile = readFileSync,
  minAvailableBytes = 6 * 1024 ** 3,
  maxMemoryPsiAvg10 = 2
} = {}) {
  try {
    const availableBytes = parseMemAvailable(readFile("/proc/meminfo", "utf8"));
    const memoryPsiAvg10 = parsePsiAvg10(readFile("/proc/pressure/memory", "utf8"));
    const healthy = (availableBytes == null || availableBytes >= minAvailableBytes)
      && (memoryPsiAvg10 == null || memoryPsiAvg10 <= maxMemoryPsiAvg10);
    return { healthy, availableBytes, memoryPsiAvg10 };
  } catch {
    // Non-Linux development machines have no PSI. Capacity limits still
    // apply there; production Linux hosts add the live pressure gate.
    return { healthy: true, availableBytes: null, memoryPsiAvg10: null };
  }
}

export async function waitForSystemHeadroom({
  sample = systemHeadroom,
  pollMs = 2_000,
  onPressure = null
} = {}) {
  let reported = false;
  while (true) {
    const state = await sample();
    if (state.healthy) return state;
    if (!reported) {
      reported = true;
      onPressure?.(state);
    }
    await delay(Math.max(10, number(pollMs, 2_000)));
  }
}

export function pipelineStageWeight({ stage, sourceBytes, largePbfBytes, capacity }) {
  const total = Math.max(1, Math.floor(number(capacity, 1)));
  const large = Number(sourceBytes || 0) >= Number(largePbfBytes || Infinity);
  if (stage === "roads") {
    // Even a sub-GiB PBF can expand into a multi-GiB turn graph. Permit at
    // most two ordinary road builders on a 4-lane host; a large one reserves
    // three lanes but still leaves one place worker running.
    return large ? Math.max(1, total - 1) : Math.min(2, total);
  }
  if (!large) return 1;
  // Two large place scans may coexist on a 4-lane host. A large route build
  // keeps one lane free for place extraction/download finalization but blocks
  // another country-scale route builder.
  return Math.min(2, total);
}

export function createAdaptiveStageLimiter({ capacity, beforeStart = null } = {}) {
  if (!Number.isInteger(capacity) || capacity < 1) {
    throw new TypeError("capacity must be a positive integer");
  }
  let used = 0;
  let pumping = false;
  const waiting = [];

  const pump = async () => {
    if (pumping) return;
    pumping = true;
    try {
      while (waiting.length) {
        // A heavy road build at the head may not fit while a one-lane place
        // worker does. Start the earliest task that fits so weighted capacity
        // does not sit idle; finite region workers bound starvation.
        const index = waiting.findIndex(item => used + item.weight <= capacity);
        if (index < 0) return;
        const next = waiting[index];
        try {
          if (beforeStart) await beforeStart(next.meta);
        } catch (error) {
          waiting.splice(index, 1);
          next.reject(error);
          continue;
        }
        waiting.splice(index, 1);
        used += next.weight;
        void Promise.resolve()
          .then(next.task)
          .then(next.resolve, next.reject)
          .finally(() => {
            used -= next.weight;
            void pump();
          });
      }
    } finally {
      pumping = false;
      // A completion can occur while an asynchronous pressure gate is
      // resolving. Re-check once ownership of the pump is released.
      if (waiting.some(item => used + item.weight <= capacity)) void pump();
    }
  };

  return {
    run(requestedWeight, task, meta = {}) {
      if (typeof task !== "function") throw new TypeError("task must be a function");
      const weight = Math.max(1, Math.min(capacity, Math.floor(number(requestedWeight, 1))));
      return new Promise((resolveDone, rejectDone) => {
        waiting.push({ weight, task, meta, resolve: resolveDone, reject: rejectDone });
        void pump();
      });
    },
    get capacity() { return capacity; },
    get used() { return used; },
    get pending() { return waiting.length; }
  };
}
