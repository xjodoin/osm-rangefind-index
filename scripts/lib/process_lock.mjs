import { randomUUID } from "node:crypto";
import { closeSync, mkdirSync, openSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { dirname } from "node:path";

function loadLock(path) {
  try {
    return JSON.parse(readFileSync(path, "utf8"));
  } catch {
    return null;
  }
}

function processIsAlive(pid) {
  if (!Number.isInteger(pid) || pid <= 0) return false;
  try {
    process.kill(pid, 0);
    return true;
  } catch (error) {
    if (error.code === "ESRCH") return false;
    throw error;
  }
}

// Cross-command process lock shared by the indexer and root-only refreshes.
// Creation is exclusive, so a refresh cannot start while the indexer is
// active and a new index run cannot start during the refresh's read/merge/CAS
// window. A killed process may leave the file behind; its dead PID is safely
// reclaimed by the next caller.
export function acquireProcessLock(path, { label = "Another run", log = () => {} } = {}) {
  mkdirSync(dirname(path), { recursive: true });
  const token = randomUUID();
  for (let attempt = 0; attempt < 2; attempt++) {
    let fd;
    try {
      fd = openSync(path, "wx");
      writeFileSync(fd, JSON.stringify({ pid: process.pid, token, started: new Date().toISOString() }));
      closeSync(fd);
      let released = false;
      const release = () => {
        if (released) return;
        released = true;
        // Never remove a replacement lock if this file was externally
        // reclaimed after our process lost ownership.
        if (loadLock(path)?.token === token) rmSync(path, { force: true });
      };
      process.on("exit", release);
      return release;
    } catch (error) {
      if (fd !== undefined) {
        closeSync(fd);
        rmSync(path, { force: true });
      }
      if (error.code !== "EEXIST") throw error;
      const lock = loadLock(path);
      if (processIsAlive(Number(lock?.pid))) {
        throw new Error(`${label} is active (pid ${lock.pid}); refusing to continue.`);
      }
      if (attempt > 0) throw new Error(`Could not reclaim stale process lock ${path}.`);
      log(`Removing stale lock${lock?.pid ? ` from pid ${lock.pid}` : ""}.`);
      rmSync(path, { force: true });
    }
  }
  throw new Error(`Could not acquire process lock ${path}.`);
}
