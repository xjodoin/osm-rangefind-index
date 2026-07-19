import assert from "node:assert/strict";
import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import { createR2Store, r2ConfigFromEnv } from "../scripts/lib/r2_store.mjs";

const env = {
  R2_BUCKET: "bucket",
  R2_PREFIX: "indexes/osm",
  R2_ENDPOINT: "https://account.r2.cloudflarestorage.com",
  R2_ACCESS_KEY_ID: "access",
  R2_SECRET_ACCESS_KEY: "secret",
  R2_REQUEST_CONCURRENCY: "4"
};

test("requires direct R2 configuration and does not accept an rclone remote", () => {
  assert.throws(() => r2ConfigFromEnv({ R2_REMOTE: "r2:bucket" }), /R2_BUCKET/u);
  assert.deepEqual(r2ConfigFromEnv(env), {
    bucket: "bucket",
    prefix: "indexes/osm/",
    endpoint: env.R2_ENDPOINT,
    accessKeyId: "access",
    secretAccessKey: "secret",
    concurrency: 4,
    uploadAttempts: 6
  });
});

test("reopens file streams when an R2 edge response is transient", async () => {
  const attempts = [];
  const waits = [];
  const client = {
    send: async command => {
      let content = "";
      for await (const chunk of command.input.Body) content += chunk;
      attempts.push(content);
      if (attempts.length === 1) {
        const error = new Error("@aws-sdk XML parse error: mismatched tags <hr> and </body>.");
        error.$metadata = { httpStatusCode: 400 };
        throw error;
      }
      return {};
    },
    destroy() {}
  };
  const directory = mkdtempSync(join(tmpdir(), "r2-store-retry-test-"));
  const file = join(directory, "pack.bin");
  writeFileSync(file, "retry me");
  try {
    const store = createR2Store({
      env: { ...env, R2_UPLOAD_ATTEMPTS: "3" },
      client,
      sleep: async waitMs => waits.push(waitMs),
      onRetry: () => {}
    });
    await store.putFile(file, "shards/quebec/pack.bin");
    assert.deepEqual(attempts, ["retry me", "retry me"]);
    assert.deepEqual(waits, [500]);
  } finally {
    rmSync(directory, { recursive: true, force: true });
  }
});

test("does not retry genuine R2 request errors", async () => {
  let attempts = 0;
  const client = {
    send: async command => {
      for await (const _chunk of command.input.Body) { /* consume mocked upload */ }
      attempts++;
      const error = new Error("Access denied");
      error.$metadata = { httpStatusCode: 403 };
      throw error;
    },
    destroy() {}
  };
  const directory = mkdtempSync(join(tmpdir(), "r2-store-no-retry-test-"));
  const file = join(directory, "pack.bin");
  writeFileSync(file, "content");
  try {
    const store = createR2Store({ env, client, sleep: async () => {} });
    await assert.rejects(store.putFile(file, "pack.bin"), /Access denied/u);
    assert.equal(attempts, 1);
  } finally {
    rmSync(directory, { recursive: true, force: true });
  }
});

test("uploads files with immutable metadata and the configured key prefix", async () => {
  const commands = [];
  const client = {
    send: async command => {
      commands.push(command);
      if (command.input.Body) {
        for await (const _chunk of command.input.Body) { /* consume mocked upload */ }
      }
      return {};
    },
    destroy() {}
  };
  const directory = mkdtempSync(join(tmpdir(), "r2-store-test-"));
  const file = join(directory, "pack.bin.gz");
  writeFileSync(file, "content");
  try {
    const store = createR2Store({ env, client });
    await store.putFile(file, "shards/quebec/pack.bin.gz");
    assert.equal(commands.length, 1);
    assert.equal(commands[0].input.Bucket, "bucket");
    assert.equal(commands[0].input.Key, "indexes/osm/shards/quebec/pack.bin.gz");
    assert.equal(commands[0].input.ContentLength, 7);
    assert.equal(commands[0].input.CacheControl, "public, max-age=31536000, immutable");
  } finally {
    rmSync(directory, { recursive: true, force: true });
  }
});

test("paginates listings and batches deletes at the S3 limit", async () => {
  const commands = [];
  const client = {
    send: async command => {
      commands.push(command);
      if (command.constructor.name === "ListObjectsV2Command") {
        if (!command.input.ContinuationToken) {
          return {
            Contents: [{ Key: "indexes/osm/shards/q/one.bin", Size: 1 }],
            IsTruncated: true,
            NextContinuationToken: "next"
          };
        }
        return { Contents: [{ Key: "indexes/osm/shards/q/two.bin", Size: 2 }], IsTruncated: false };
      }
      return {};
    },
    destroy() {}
  };
  const store = createR2Store({ env, client });
  assert.deepEqual(await store.listObjects("shards/q/"), [
    { path: "shards/q/one.bin", size: 1, modTime: null, etag: null },
    { path: "shards/q/two.bin", size: 2, modTime: null, etag: null }
  ]);
  await store.deleteObjects(Array.from({ length: 1001 }, (_, index) => `garbage/${index}.bin`));
  const deletes = commands.filter(command => command.constructor.name === "DeleteObjectsCommand");
  assert.equal(deletes.length, 2);
  assert.equal(deletes[0].input.Delete.Objects.length, 1000);
  assert.equal(deletes[1].input.Delete.Objects.length, 1);
});
