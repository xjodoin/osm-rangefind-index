import { setTimeout as delay } from "node:timers/promises";

export class DiskHeadroomError extends Error {}

function gib(bytes) {
  return `${(Number(bytes || 0) / 1024 ** 3).toFixed(1)} GiB`;
}

/**
 * Atomically admits extraction work against live free space plus reservations.
 * A caller that cannot fit waits only while active leases can release space;
 * once none remain, insufficient capacity is a genuine disk-size failure.
 */
export function createDiskAdmissionController({
  minFreeBytes,
  freeBytes,
  workingBytes,
  pollMs = 2_000,
  sleep = delay
}) {
  let reservedBytes = 0;
  let leases = 0;
  const notify = (callback, value) => {
    try { callback?.(value); } catch { /* admission telemetry must never affect work */ }
  };

  async function waitForCapacity({ region, reservationBytes, currentBytes = 0, shouldStop, onWait }) {
    let waitReported = false;
    while (!shouldStop()) {
      const availableBytes = Math.max(0, Number(freeBytes()) || 0);
      const nextReservedBytes = reservedBytes - currentBytes + reservationBytes;
      const requiredBytes = Number(minFreeBytes) + nextReservedBytes;
      if (availableBytes >= requiredBytes) return { reservationBytes, nextReservedBytes };
      const releasableLeases = leases - (currentBytes > 0 ? 1 : 0);
      if (releasableLeases < 1) {
        throw new DiskHeadroomError(
          `${region.id}: ${gib(availableBytes)} free; ${gib(requiredBytes)} required before extraction`
        );
      }
      if (!waitReported) {
        waitReported = true;
        notify(onWait, {
          availableBytes,
          requiredBytes,
          reservationBytes,
          reservedBytes,
          leases: releasableLeases
        });
      }
      await sleep(Math.max(10, Number(pollMs) || 2_000));
    }
    return null;
  }

  return {
    async acquire({ region, sourceBytes = 0, shouldStop = () => false, onWait = null } = {}) {
      const reservationBytes = Math.max(1, Number(workingBytes(region, sourceBytes)) || 0);
      const admitted = await waitForCapacity({ region, reservationBytes, shouldStop, onWait });
      if (!admitted) return null;
      // No await occurs between the capacity check and mutation, so all
      // contenders observe an atomic admission on the Node event loop.
      reservedBytes = admitted.nextReservedBytes;
      leases++;
      let heldBytes = reservationBytes;
      let released = false;
      return {
        get bytes() { return heldBytes; },
        async resize(nextSourceBytes, options = {}) {
          if (released) throw new Error(`${region.id}: cannot resize a released disk admission lease`);
          const nextBytes = Math.max(1, Number(workingBytes(region, nextSourceBytes)) || 0);
          if (nextBytes <= heldBytes) return true;
          const resized = await waitForCapacity({
            region,
            reservationBytes: nextBytes,
            currentBytes: heldBytes,
            shouldStop: options.shouldStop || shouldStop,
            onWait: options.onWait || onWait
          });
          if (!resized) return false;
          reservedBytes = resized.nextReservedBytes;
          heldBytes = nextBytes;
          return true;
        },
        release() {
          if (released) return;
          released = true;
          reservedBytes -= heldBytes;
          leases--;
        }
      };
    },
    get reservedBytes() { return reservedBytes; },
    get leases() { return leases; }
  };
}
