export function createSerialTaskQueue({ maxPending = 2 } = {}) {
  if (!Number.isInteger(maxPending) || maxPending < 1) {
    throw new TypeError("maxPending must be a positive integer");
  }

  const pending = new Set();
  let tail = Promise.resolve();
  let failure = null;

  const throwIfFailed = () => {
    if (failure) throw failure;
  };

  return {
    async enqueue(task) {
      if (typeof task !== "function") {
        throw new TypeError("task must be a function");
      }
      throwIfFailed();
      while (pending.size >= maxPending) {
        await Promise.race(pending);
        throwIfFailed();
      }

      let queued;
      queued = tail
        .then(async () => {
          if (failure) return;
          try {
            await task();
          } catch (error) {
            failure ||= error;
          }
        })
        .finally(() => pending.delete(queued));
      tail = queued;
      pending.add(queued);
    },

    async drain() {
      await Promise.all(pending);
      throwIfFailed();
    },

    get pending() {
      return pending.size;
    },

    get capacity() {
      return maxPending;
    }
  };
}
