import assert from "node:assert/strict";
import test from "node:test";
import {
  createDiskAdmissionController,
  DiskHeadroomError
} from "../scripts/lib/disk_admission.mjs";

const GiB = 1024 ** 3;
const region = id => ({ id, addressSources: [{}] });

test("waits for active cleanup reservation and admits atomically after release", async () => {
  let available = 55 * GiB;
  let wake;
  const controller = createDiskAdmissionController({
    minFreeBytes: 24 * GiB,
    freeBytes: () => available,
    workingBytes: () => 16 * GiB,
    sleep: () => new Promise(resolve => { wake = resolve; })
  });
  const first = await controller.acquire({ region: region("first") });
  let waited = 0;
  const secondPromise = controller.acquire({
    region: region("second"),
    onWait: () => { waited++; }
  });
  await new Promise(resolve => setImmediate(resolve));
  assert.equal(waited, 1);
  assert.equal(controller.reservedBytes, 16 * GiB);

  // Cleanup releases both its reservation and real scratch space.
  available = 64 * GiB;
  first.release();
  wake();
  const second = await secondPromise;
  assert.equal(controller.leases, 1);
  assert.equal(controller.reservedBytes, 16 * GiB);
  second.release();
  assert.equal(controller.leases, 0);
});

test("fails immediately when disk is genuinely insufficient without active cleanup", async () => {
  const controller = createDiskAdmissionController({
    minFreeBytes: 24 * GiB,
    freeBytes: () => 30 * GiB,
    workingBytes: () => 16 * GiB
  });
  await assert.rejects(
    controller.acquire({ region: region("too-large") }),
    error => error instanceof DiskHeadroomError && /30\.0 GiB free; 40\.0 GiB required/u.test(error.message)
  );
});

test("returns without a lease when the nightly deadline arrives while waiting", async () => {
  let stop = false;
  const controller = createDiskAdmissionController({
    minFreeBytes: 24 * GiB,
    freeBytes: () => 50 * GiB,
    workingBytes: () => 16 * GiB,
    sleep: async () => { stop = true; }
  });
  const first = await controller.acquire({ region: region("active") });
  const waiting = await controller.acquire({ region: region("waiting"), shouldStop: () => stop });
  assert.equal(waiting, null);
  first.release();
});

test("resizes a held lease atomically after another cleanup completes", async () => {
  let available = 60 * GiB;
  let wake;
  const controller = createDiskAdmissionController({
    minFreeBytes: 24 * GiB,
    freeBytes: () => available,
    workingBytes: (_region, sourceBytes) => sourceBytes ? 24 * GiB : 16 * GiB,
    sleep: () => new Promise(resolve => { wake = resolve; })
  });
  const first = await controller.acquire({ region: region("first") });
  const growing = await controller.acquire({ region: region("growing") });
  const resize = growing.resize(1, { onWait: state => assert.equal(state.leases, 1) });
  await new Promise(resolve => setImmediate(resolve));
  assert.equal(growing.bytes, 16 * GiB);
  assert.equal(controller.reservedBytes, 32 * GiB);
  first.release();
  wake();
  assert.equal(await resize, true);
  assert.equal(growing.bytes, 24 * GiB);
  assert.equal(controller.reservedBytes, 24 * GiB);
  growing.release();
});

test("fails a lease resize only when no other cleanup can make it fit", async () => {
  const controller = createDiskAdmissionController({
    minFreeBytes: 24 * GiB,
    freeBytes: () => 45 * GiB,
    workingBytes: (_region, sourceBytes) => sourceBytes ? 24 * GiB : 16 * GiB
  });
  const lease = await controller.acquire({ region: region("growing") });
  await assert.rejects(lease.resize(1), DiskHeadroomError);
  assert.equal(lease.bytes, 16 * GiB);
  assert.equal(controller.reservedBytes, 16 * GiB);
  lease.release();
});
