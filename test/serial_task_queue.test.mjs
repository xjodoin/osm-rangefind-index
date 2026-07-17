import assert from "node:assert/strict";
import test from "node:test";
import { createSerialTaskQueue } from "../scripts/lib/serial_task_queue.mjs";

function deferred() {
  let resolve;
  const promise = new Promise(done => {
    resolve = done;
  });
  return { promise, resolve };
}

test("runs tasks serially while enqueue allows foreground work to continue", async () => {
  const queue = createSerialTaskQueue({ maxPending: 2 });
  const firstGate = deferred();
  const events = [];

  await queue.enqueue(async () => {
    events.push("first:start");
    await firstGate.promise;
    events.push("first:end");
  });
  await queue.enqueue(async () => {
    events.push("second:start");
    events.push("second:end");
  });

  await Promise.resolve();
  assert.deepEqual(events, ["first:start"]);
  assert.equal(queue.pending, 2);

  const thirdAccepted = queue.enqueue(async () => events.push("third"));
  let accepted = false;
  void thirdAccepted.then(() => {
    accepted = true;
  });
  await Promise.resolve();
  assert.equal(
    accepted,
    false,
    "capacity must backpressure a third pending task"
  );

  firstGate.resolve();
  await thirdAccepted;
  await queue.drain();
  assert.deepEqual(events, [
    "first:start",
    "first:end",
    "second:start",
    "second:end",
    "third"
  ]);
});

test("surfaces the first background failure and skips queued work", async () => {
  const queue = createSerialTaskQueue({ maxPending: 2 });
  let secondRan = false;
  await queue.enqueue(async () => {
    throw new Error("upload failed");
  });
  await queue.enqueue(async () => {
    secondRan = true;
  });

  await assert.rejects(queue.drain(), /upload failed/u);
  assert.equal(secondRan, false);
  await assert.rejects(queue.enqueue(async () => {}), /upload failed/u);
});
