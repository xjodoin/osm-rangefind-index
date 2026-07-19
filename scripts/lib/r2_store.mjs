import {
  DeleteObjectsCommand,
  GetObjectCommand,
  HeadObjectCommand,
  ListObjectsV2Command,
  PutObjectCommand,
  S3Client
} from "@aws-sdk/client-s3";
import { createReadStream, createWriteStream, mkdirSync, readdirSync, renameSync, statSync } from "node:fs";
import { dirname, join, posix, relative } from "node:path";
import { pipeline } from "node:stream/promises";
import { setTimeout as delay } from "node:timers/promises";

const IMMUTABLE_OBJECT = /\.bin(?:\.gz)?$/u;
const TRANSIENT_NETWORK_ERRORS = new Set([
  "ECONNRESET",
  "ECONNREFUSED",
  "EPIPE",
  "ETIMEDOUT",
  "EAI_AGAIN",
  "ENETDOWN",
  "ENETUNREACH",
  "UND_ERR_CONNECT_TIMEOUT",
  "UND_ERR_HEADERS_TIMEOUT",
  "UND_ERR_SOCKET"
]);

function normalizePrefix(value) {
  const prefix = String(value || "").replace(/^\/+|\/+$/gu, "");
  return prefix ? `${prefix}/` : "";
}

export function r2ConfigFromEnv(env = process.env) {
  const bucket = env.R2_BUCKET || "";
  const prefix = env.R2_PREFIX || "";
  const endpoint = env.R2_ENDPOINT || "";
  const accessKeyId = env.R2_ACCESS_KEY_ID || "";
  const secretAccessKey = env.R2_SECRET_ACCESS_KEY || "";
  const concurrency = Math.max(1, Math.min(64, Number(env.R2_REQUEST_CONCURRENCY || 16) || 16));
  const uploadAttempts = Math.max(1, Math.min(10, Number(env.R2_UPLOAD_ATTEMPTS || 6) || 6));
  if (!bucket) throw new Error("Set R2_BUCKET.");
  if (!endpoint) throw new Error("Set R2_ENDPOINT.");
  if (!accessKeyId || !secretAccessKey) throw new Error("Set R2_ACCESS_KEY_ID and R2_SECRET_ACCESS_KEY.");
  return { bucket, prefix: normalizePrefix(prefix), endpoint, accessKeyId, secretAccessKey, concurrency, uploadAttempts };
}

function isRetryableUploadError(error) {
  const status = Number(error?.$metadata?.httpStatusCode || 0);
  const message = String(error?.message || "");
  return Boolean(error?.$retryable)
    || TRANSIENT_NETWORK_ERRORS.has(error?.code)
    || status === 408
    || status === 429
    || status >= 500
    // R2 occasionally returns an HTML edge-error response with status 400.
    // The S3 XML parser then fails before it can expose a service error code.
    || (status === 400 && /XML parse error|Deserialization error|mismatched tags/iu.test(message));
}

function createLimiter(limit) {
  let active = 0;
  const waiting = [];
  const drain = () => {
    while (active < limit && waiting.length) {
      active++;
      const next = waiting.shift();
      void next().finally(() => {
        active--;
        drain();
      });
    }
  };
  return fn => new Promise((resolveDone, rejectDone) => {
    waiting.push(async () => {
      try {
        resolveDone(await fn());
      } catch (error) {
        rejectDone(error);
      }
    });
    drain();
  });
}

export function listLocalFiles(root) {
  const files = [];
  const walk = directory => {
    for (const entry of readdirSync(directory, { withFileTypes: true })) {
      const path = join(directory, entry.name);
      if (entry.isDirectory()) walk(path);
      else if (entry.isFile()) files.push({ path, relative: relative(root, path).split("\\").join("/") });
    }
  };
  walk(root);
  return files;
}

function metadataFor(path) {
  if (IMMUTABLE_OBJECT.test(path)) {
    return { ContentType: "application/octet-stream", CacheControl: "public, max-age=31536000, immutable" };
  }
  if (path.endsWith(".json")) return { ContentType: "application/json", CacheControl: "no-cache" };
  if (path.endsWith(".html")) return { ContentType: "text/html; charset=utf-8", CacheControl: "no-cache" };
  return { ContentType: "application/octet-stream" };
}

export function createR2Store({
  env = process.env,
  client = null,
  sleep = delay,
  onRetry = ({ path, attempt, attempts, waitMs, error }) => {
    console.warn(`[r2] ${path}: transient upload failure (${error.message}); retry ${attempt + 1}/${attempts} in ${waitMs}ms.`);
  }
} = {}) {
  const config = r2ConfigFromEnv(env);
  const s3 = client || new S3Client({
    region: "auto",
    endpoint: config.endpoint,
    forcePathStyle: true,
    requestChecksumCalculation: "WHEN_REQUIRED",
    responseChecksumValidation: "WHEN_REQUIRED",
    credentials: {
      accessKeyId: config.accessKeyId,
      secretAccessKey: config.secretAccessKey
    }
  });
  const limited = createLimiter(config.concurrency);
  const keyOf = path => {
    const raw = String(path || "").replace(/^\/+/, "");
    if (raw.split("/").includes("..")) throw new Error(`Unsafe R2 object path: ${path}`);
    const normalized = raw ? posix.normalize(raw) : "";
    return `${config.prefix}${normalized === "." ? "" : normalized}`;
  };
  const pathOf = key => config.prefix && key.startsWith(config.prefix) ? key.slice(config.prefix.length) : key;

  const send = command => limited(() => s3.send(command));

  async function putFile(localPath, remotePath) {
    return limited(async () => {
      const stat = statSync(localPath);
      for (let attempt = 1; ; attempt++) {
        const body = createReadStream(localPath);
        try {
          await s3.send(new PutObjectCommand({
            Bucket: config.bucket,
            Key: keyOf(remotePath),
            Body: body,
            ContentLength: stat.size,
            ...metadataFor(remotePath)
          }));
          break;
        } catch (error) {
          body.destroy();
          if (attempt >= config.uploadAttempts || !isRetryableUploadError(error)) throw error;
          const waitMs = Math.min(8_000, 500 * (2 ** (attempt - 1)));
          onRetry?.({ path: remotePath, attempt, attempts: config.uploadAttempts, waitMs, error });
          await sleep(waitMs);
        }
      }
      return { path: remotePath, bytes: stat.size };
    });
  }

  async function putBytes(remotePath, body) {
    const bytes = Buffer.isBuffer(body) ? body : Buffer.from(body);
    await send(new PutObjectCommand({
      Bucket: config.bucket,
      Key: keyOf(remotePath),
      Body: bytes,
      ContentLength: bytes.length,
      ...metadataFor(remotePath)
    }));
    return { path: remotePath, bytes: bytes.length };
  }

  async function putFiles(files, remotePrefix = "", onUploaded = null) {
    let bytes = 0;
    await Promise.all(files.map(async file => {
      const remotePath = posix.join(remotePrefix, file.relative);
      const result = await putFile(file.path, remotePath);
      bytes += result.bytes;
      onUploaded?.(result);
    }));
    return { files: files.length, bytes };
  }

  async function getText(path) {
    return limited(async () => {
      const response = await s3.send(new GetObjectCommand({ Bucket: config.bucket, Key: keyOf(path) }));
      return response.Body.transformToString("utf-8");
    });
  }

  async function getFile(path, localPath) {
    return limited(async () => {
      const response = await s3.send(new GetObjectCommand({ Bucket: config.bucket, Key: keyOf(path) }));
      mkdirSync(dirname(localPath), { recursive: true });
      const temp = `${localPath}.download`;
      await pipeline(response.Body, createWriteStream(temp));
      renameSync(temp, localPath);
    });
  }

  async function exists(path) {
    try {
      await send(new HeadObjectCommand({ Bucket: config.bucket, Key: keyOf(path) }));
      return true;
    } catch (error) {
      if (error?.$metadata?.httpStatusCode === 404 || error?.name === "NotFound") return false;
      throw error;
    }
  }

  async function listObjects(prefix = "") {
    const objects = [];
    let continuationToken;
    do {
      const response = await send(new ListObjectsV2Command({
        Bucket: config.bucket,
        Prefix: keyOf(prefix),
        ContinuationToken: continuationToken
      }));
      for (const object of response.Contents || []) {
        objects.push({
          path: pathOf(object.Key),
          size: Number(object.Size || 0),
          modTime: object.LastModified?.toISOString() || null,
          etag: object.ETag || null
        });
      }
      continuationToken = response.IsTruncated ? response.NextContinuationToken : undefined;
    } while (continuationToken);
    return objects;
  }

  async function listLevel(prefix = "") {
    const prefixes = [];
    const objects = [];
    let continuationToken;
    do {
      const response = await send(new ListObjectsV2Command({
        Bucket: config.bucket,
        Prefix: keyOf(prefix),
        Delimiter: "/",
        ContinuationToken: continuationToken
      }));
      for (const item of response.CommonPrefixes || []) prefixes.push(pathOf(item.Prefix));
      for (const object of response.Contents || []) {
        objects.push({
          path: pathOf(object.Key),
          size: Number(object.Size || 0),
          modTime: object.LastModified?.toISOString() || null,
          etag: object.ETag || null
        });
      }
      continuationToken = response.IsTruncated ? response.NextContinuationToken : undefined;
    } while (continuationToken);
    return { prefixes, objects };
  }

  async function listCommonPrefixes(prefix = "") {
    return (await listLevel(prefix)).prefixes;
  }

  async function downloadPrefix(prefix, localDir, include = () => true) {
    const objects = (await listObjects(prefix)).filter(object => include(object.path.slice(prefix.length)));
    await Promise.all(objects.map(object => getFile(object.path, join(localDir, object.path.slice(prefix.length)))));
    return { files: objects.length, bytes: objects.reduce((sum, object) => sum + object.size, 0) };
  }

  async function deleteObjects(paths) {
    let deleted = 0;
    for (let offset = 0; offset < paths.length; offset += 1000) {
      const batch = paths.slice(offset, offset + 1000);
      const response = await send(new DeleteObjectsCommand({
        Bucket: config.bucket,
        Delete: { Quiet: true, Objects: batch.map(path => ({ Key: keyOf(path) })) }
      }));
      if (response.Errors?.length) {
        throw new Error(`R2 failed to delete ${response.Errors.length} object(s): ${response.Errors[0].Key} ${response.Errors[0].Message}`);
      }
      deleted += batch.length;
    }
    return deleted;
  }

  return {
    bucket: config.bucket,
    prefix: config.prefix,
    putFile,
    putBytes,
    putFiles,
    getText,
    getFile,
    exists,
    listObjects,
    listLevel,
    listCommonPrefixes,
    downloadPrefix,
    deleteObjects,
    close: () => s3.destroy()
  };
}
