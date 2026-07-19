export function createTaskQueue({ maxPending = 2, concurrency = 1 } = {}) {
  if (!Number.isInteger(maxPending) || maxPending < 1) {
    throw new TypeError("maxPending must be a positive integer");
  }
  if (!Number.isInteger(concurrency) || concurrency < 1 || concurrency > maxPending) {
    throw new TypeError("concurrency must be a positive integer no greater than maxPending");
  }

  const pending = new Set();
  const waiting = [];
  let active = 0;
  let failure = null;

  const throwIfFailed = () => {
    if (failure) throw failure;
  };

  const pump = () => {
    while (!failure && active < concurrency && waiting.length) {
      const item = waiting.shift();
      active++;
      void Promise.resolve().then(item.run).finally(() => {
        active--;
        pending.delete(item.done);
        item.resolveDone();
        pump();
      });
    }
    if (failure) {
      while (waiting.length) {
        const item = waiting.shift();
        pending.delete(item.done);
        item.resolveDone();
      }
    }
  };

  return {
    async enqueue(task) {
      if (typeof task !== "function") throw new TypeError("task must be a function");
      throwIfFailed();
      while (pending.size >= maxPending) {
        await Promise.race(pending);
        throwIfFailed();
      }

      let resolveDone;
      const done = new Promise(resolve => {
        resolveDone = resolve;
      });
      pending.add(done);
      waiting.push({
        done,
        resolveDone,
        run: async () => {
          try {
            await task();
          } catch (error) {
            failure ||= error;
          }
        }
      });
      pump();
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
    },

    get concurrency() {
      return concurrency;
    }
  };
}

export function createSerialTaskQueue({ maxPending = 2 } = {}) {
  return createTaskQueue({ maxPending, concurrency: 1 });
}
