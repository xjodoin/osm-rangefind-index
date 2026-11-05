import assert from "node:assert/strict";
import { execFileSync, fork } from "node:child_process";
import { createReadStream, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { createInterface } from "node:readline";
import { fileURLToPath } from "node:url";
import { createGunzip } from "node:zlib";
import test from "node:test";
import { gzipFileParallel } from "../scripts/lib/parallel_gzip.mjs";

const projectRoot = dirname(dirname(fileURLToPath(import.meta.url)));

function corpus(lines) {
  return Array.from({ length: lines }, (_, i) => JSON.stringify({ id: `n${i}`, name: `Place ${i}`, v: i * 7 })).join("\n") + "\n";
}

async function gunzipLines(path) {
  const lines = [];
  for await (const line of createInterface({ input: createReadStream(path).pipe(createGunzip()), crlfDelay: Infinity })) {
    lines.push(line);
  }
  return lines;
}

test("multi-member output round-trips through createGunzip line by line", async () => {
  const directory = mkdtempSync(join(tmpdir(), "parallel-gzip-test-"));
  try {
    const input = join(directory, "corpus.jsonl");
    const output = join(directory, "corpus.jsonl.gz");
    const text = corpus(20_000);
    writeFileSync(input, text);
    // Tiny blocks force many members, with boundaries falling mid-line.
    const result = await gzipFileParallel(input, output, { workers: 3, blockBytes: 4099 });
    assert.equal(result.inputBytes, Buffer.byteLength(text));
    assert.ok(result.outputBytes > 0);
    assert.deepEqual(await gunzipLines(output), text.trimEnd().split("\n"));
    assert.equal(execFileSync("gzip", ["-dc", output], { maxBuffer: 64 << 20 }).toString(), text);
  } finally {
    rmSync(directory, { recursive: true, force: true });
  }
});

test("an empty corpus compresses to a readable empty stream", async () => {
  const directory = mkdtempSync(join(tmpdir(), "parallel-gzip-empty-test-"));
  try {
    const input = join(directory, "empty.jsonl");
    const output = join(directory, "empty.jsonl.gz");
    writeFileSync(input, "");
    await gzipFileParallel(input, output);
    assert.deepEqual(await gunzipLines(output), []);
  } finally {
    rmSync(directory, { recursive: true, force: true });
  }
});

test("the compression worker reports sizes over IPC", async () => {
  const directory = mkdtempSync(join(tmpdir(), "parallel-gzip-worker-test-"));
  try {
    const input = join(directory, "corpus.jsonl");
    const output = join(directory, "corpus.jsonl.gz");
    const text = corpus(5_000);
    writeFileSync(input, text);
    const value = await new Promise((resolveDone, rejectDone) => {
      let result;
      const child = fork(join(projectRoot, "scripts/compress_corpus_worker.mjs"), [input, output, "6", "2"], {
        stdio: ["ignore", "ignore", "inherit", "ipc"],
        env: { ...process.env, UV_THREADPOOL_SIZE: "3" }
      });
      child.on("message", message => {
        if (message?.type === "result") result = message.value;
      });
      child.on("error", rejectDone);
      child.on("exit", code => (code === 0 && result ? resolveDone(result) : rejectDone(new Error(`exit ${code}`))));
    });
    assert.equal(value.inputBytes, Buffer.byteLength(text));
    assert.equal(value.outputBytes, readFileSync(output).length);
    assert.deepEqual(await gunzipLines(output), text.trimEnd().split("\n"));
  } finally {
    rmSync(directory, { recursive: true, force: true });
  }
});
