import http from "node:http";

const port = Number.parseInt(process.env.PORT || "18083", 10);
const cpuBase = (process.env.RERANK_CPU_URL || "http://qwen-reranker:8000").replace(/\/$/, "");
const gpuBase = (process.env.RERANK_GPU_URL || "http://qwen-reranker-gpu:8000").replace(/\/$/, "");
const maxBodyBytes = Number.parseInt(process.env.MAX_BODY_BYTES || String(16 * 1024 * 1024), 10);
const healthTimeoutMs = Number.parseInt(process.env.GPU_HEALTH_TIMEOUT_MS || "350", 10);

async function getHealth(baseURL) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), healthTimeoutMs);
  try {
    const response = await fetch(`${baseURL}/health`, { signal: controller.signal });
    if (!response.ok) return { ok: false };
    let detail = {};
    try {
      detail = await response.json();
    } catch {
      // A 2xx health response is sufficient even when the backend has no JSON body.
    }
    return { ok: true, detail };
  } catch {
    return { ok: false };
  } finally {
    clearTimeout(timer);
  }
}

async function readBody(request) {
  const chunks = [];
  let size = 0;
  for await (const chunk of request) {
    size += chunk.length;
    if (size > maxBodyBytes) {
      throw new Error("request body too large");
    }
    chunks.push(chunk);
  }
  return Buffer.concat(chunks);
}

function forwardHeaders(headers) {
  const forwarded = {};
  for (const [name, value] of Object.entries(headers)) {
    const lower = name.toLowerCase();
    if (["host", "connection", "content-length", "transfer-encoding"].includes(lower)) continue;
    if (value !== undefined) forwarded[name] = value;
  }
  return forwarded;
}

async function callBackend(baseURL, request, body) {
  return fetch(`${baseURL}${request.url}`, {
    method: request.method,
    headers: forwardHeaders(request.headers),
    body: request.method === "GET" || request.method === "HEAD" ? undefined : body,
  });
}

async function writeFetchResponse(response, result, backend) {
  response.statusCode = result.status;
  for (const [name, value] of result.headers.entries()) {
    if (!["connection", "content-length", "transfer-encoding"].includes(name.toLowerCase())) {
      response.setHeader(name, value);
    }
  }
  response.setHeader("x-weknora-rerank-backend", backend);
  const buffer = await result.arrayBuffer();
  response.end(Buffer.from(buffer));
}

const server = http.createServer(async (request, response) => {
  try {
    if (request.url === "/health") {
      const [gpu, cpu] = await Promise.all([getHealth(gpuBase), getHealth(cpuBase)]);
      response.writeHead(cpu.ok ? 200 : 503, { "content-type": "application/json" });
      response.end(JSON.stringify({
        status: cpu.ok ? "ok" : "degraded",
        selected: gpu.ok ? "gpu" : "cpu",
        gpu: gpu.ok,
        cpu: cpu.ok,
        gpu_detail: gpu.detail || null,
        cpu_detail: cpu.detail || null,
      }));
      return;
    }

    const body = await readBody(request);
    const gpuHealth = await getHealth(gpuBase);
    if (gpuHealth.ok) {
      try {
        const gpuResponse = await callBackend(gpuBase, request, body);
        if (gpuResponse.status < 500) {
          await writeFetchResponse(response, gpuResponse, "gpu");
          return;
        }
      } catch {
        // The CPU path below is the fail-closed fallback for transient GPU loss.
      }
    }

    const cpuResponse = await callBackend(cpuBase, request, body);
    await writeFetchResponse(response, cpuResponse, "cpu");
  } catch (error) {
    const status = error?.message === "request body too large" ? 413 : 503;
    response.writeHead(status, { "content-type": "application/json" });
    response.end(JSON.stringify({ error: error?.message || "accelerator router failure" }));
  }
});

server.listen(port, "0.0.0.0", () => {
  console.log(`adaptive accelerator router listening on ${port}`);
});
