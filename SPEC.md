# Handoff: Rust OpenAI-Compatible HTTP Gateway for Codex CLI

## Objective

Build a small, memory-efficient Rust HTTP service that exposes an OpenAI-compatible API backed by the locally installed Codex CLI.

The service must:

* Run in a container.
* Use the host machine’s existing Codex CLI authentication through a bind-mounted `CODEX_HOME`.
* Use `codex app-server` as the primary backend.
* Optionally use `codex exec --json` as an internal fallback.
* Expose only an OpenAI-compatible HTTP surface.
* Require no LiteLLM dependency.
* Require no workspace parameter.
* Avoid exposing Codex-specific thread, filesystem, approval, sandbox, or process concepts to clients.

The intended consumer can be:

* n8n.
* OpenAI SDKs.
* Agent frameworks.
* Existing applications configured with a custom OpenAI base URL.
* LiteLLM later, as an optional external proxy.

---

# Final Architecture

```text
OpenAI-compatible client
        │
        │ HTTP + SSE
        ▼
Rust Codex Gateway
        │
        ├── codex app-server
        │     persistent primary backend
        │
        └── codex exec --json
              optional internal fallback
        │
        ▼
Host-mounted Codex authentication
```

The container includes:

* The compiled Rust gateway.
* A pinned Codex CLI version.
* A persistent `codex app-server` child process.
* A neutral internal working directory.
* The host-mounted Codex authentication directory.

---

# Explicit Scope

## Included

Implement only:

```text
GET  /health
GET  /ready
GET  /v1/models
POST /v1/chat/completions
POST /v1/responses
```

Support:

* Non-streaming responses.
* Streaming SSE responses.
* OpenAI-compatible request and response shapes.
* Host Codex authentication.
* Persistent app-server process management.
* Internal Codex thread creation.
* Optional internal `codex exec` fallback.
* Bearer-token authentication for the gateway.
* Containerization and Docker Compose.
* Tests and documentation.

## Excluded from v1

Do not implement:

```text
/v1/codex/*
/threads/*
/workspaces/*
```

Do not expose:

* Workspace IDs.
* Filesystem paths.
* `cwd`.
* Codex thread IDs as required client inputs.
* Sandbox controls.
* Approval policies.
* Command approval endpoints.
* Native Codex events.
* Diff endpoints.
* File-change endpoints.
* LiteLLM configuration.
* Multi-user thread persistence.
* Repository mounting or project selection.
* Arbitrary model-provider configuration.

The implementation can use these concepts internally, but they must not appear in the public API.

---

# No Workspace Requirement

The HTTP client must not provide a workspace or working directory.

Codex still runs within a current working directory because every process has one. The gateway should manage this internally.

Use a neutral container directory:

```text
/home/codex/runtime
```

Create it during image construction and configure both:

* `codex app-server`
* `codex exec`

to run from that directory.

Do not mount source-code directories in v1.

Do not allow the model to access arbitrary host files.

Suggested container setup:

```dockerfile
WORKDIR /home/codex/runtime
```

For the persistent process:

```rust
Command::new("codex")
    .arg("app-server")
    .current_dir("/home/codex/runtime");
```

For the fallback:

```rust
Command::new("codex")
    .args(["exec", "--json", "-"])
    .current_dir("/home/codex/runtime");
```

When calling app-server `thread/start`, omit `cwd`.

Example internal request:

```json
{
  "method": "thread/start",
  "id": 1,
  "params": {
    "model": "gpt-5.6-luna"
  }
}
```

The official app-server protocol supports starting a thread without specifying `cwd`; `cwd` is an optional override rather than a required client concern.

---

# Public API Contract

## Health

```text
GET /health
```

Return whether the Rust process is alive.

Example:

```json
{
  "status": "ok"
}
```

This endpoint should not depend on Codex being ready.

---

## Readiness

```text
GET /ready
```

Return success only when:

* The Codex binary is available.
* Host authentication appears usable.
* The app-server process is running.
* The initialization handshake has completed.

Example success:

```json
{
  "status": "ready",
  "backend": "app-server"
}
```

Example unavailable:

```json
{
  "status": "unavailable",
  "reason": "codex app-server is restarting"
}
```

Use HTTP `503` when unavailable.

Do not expose credentials, paths, raw stderr, or auth tokens.

---

## Models

```text
GET /v1/models
```

Return an OpenAI-compatible model listing.

For v1, expose a small configured model list rather than dynamically forwarding the entire Codex model catalog.

Example:

```json
{
  "object": "list",
  "data": [
    {
      "id": "codex",
      "object": "model",
      "owned_by": "openai"
    },
    { "id": "gpt-5.4-mini", "object": "model", "owned_by": "openai" },
    { "id": "gpt-5.5", "object": "model", "owned_by": "openai" },
    { "id": "gpt-5.6-luna", "object": "model", "owned_by": "openai" },
    { "id": "gpt-5.6-terra", "object": "model", "owned_by": "openai" },
    { "id": "gpt-5.6-sol", "object": "model", "owned_by": "openai" }
  ]
}
```

Support a model alias:

```text
codex → configured default Codex model
```

Configure it through:

```text
CODEX_DEFAULT_MODEL
```

Do not silently accept unknown models.

Return an OpenAI-style invalid-request error for unsupported model names.

---

# Chat Completions API

Implement:

```text
POST /v1/chat/completions
```

## Supported request fields

Support at least:

```json
{
  "model": "codex",
  "messages": [
    {
      "role": "user",
      "content": "Explain how Tokio task cancellation works."
    }
  ],
  "stream": false,
  "temperature": 0.2,
  "max_tokens": 2000,
  "user": "optional-user-reference"
}
```

Required:

* `model`
* `messages`

Optional:

* `stream`
* `temperature`
* `max_tokens`
* `user`
* `stop`

Fields unsupported by Codex should either:

* Be safely ignored and documented.
* Or return a clear unsupported-parameter error.

Prefer ignoring common harmless generation fields in v1 when there is no faithful Codex equivalent.

Do not accept any custom `codex`, `workspace`, `cwd`, `sandbox`, or `thread_id` fields.

---

## Message content

Support string content:

```json
{
  "role": "user",
  "content": "Hello"
}
```

Also support OpenAI-style text and image content arrays:

```json
{
  "role": "user",
  "content": [
    {
      "type": "text",
      "text": "Hello"
    }
  ]
}
```

Image parts use Chat Completions `image_url` or Responses `input_image` shapes and are forwarded as app-server `{ "type": "image", "url": "..." }` input items. Accept HTTPS and image data URLs; do not accept public filesystem paths. Reject audio, generic files, and unresolved Files API `file_id` references with a structured error.

Supported roles:

* `system`
* `developer`
* `user`
* `assistant`

---

# Prompt Construction

Chat Completions messages need to be translated into one Codex input because the client is not directly controlling Codex threads.

Use a deterministic prompt serializer.

Example:

```text
<system>
You are a concise technical assistant.
</system>

<developer>
Prefer Rust examples.
</developer>

<conversation>
<user>
What is a bounded Tokio channel?
</user>

<assistant>
A bounded Tokio channel...
</assistant>

<user>
Show an example.
</user>
</conversation>
```

Requirements:

* Preserve message ordering.
* Escape or delimit content safely.
* Do not concatenate roles ambiguously.
* Put the newest user request last.
* Avoid adding unnecessary gateway instructions.
* Unit-test prompt construction.

For every independent HTTP request, create a fresh internal Codex thread.

Do not require the caller to know or reuse a Codex thread ID.

The gateway may include the internal thread ID in logs at debug level, but not in the standard response body.

---

# Non-Streaming Chat Completion

Return a standard-compatible response:

```json
{
  "id": "chatcmpl_<uuid>",
  "object": "chat.completion",
  "created": 1783862400,
  "model": "codex",
  "choices": [
    {
      "index": 0,
      "message": {
        "role": "assistant",
        "content": "The response..."
      },
      "finish_reason": "stop"
    }
  ],
  "usage": {
    "prompt_tokens": 0,
    "completion_tokens": 0,
    "total_tokens": 0
  }
}
```

If accurate token usage is unavailable:

* Prefer omitting `usage`, if client compatibility permits.
* Otherwise return `null`.
* Do not invent token counts.
* Do not return misleading zeros unless clearly documented as unavailable.

For a non-streaming request, accumulating the final assistant output is acceptable.

Apply a configurable response-size limit.

---

# Streaming Chat Completion

For:

```json
{
  "stream": true
}
```

return:

```text
Content-Type: text/event-stream
```

Use standard SSE chunks:

```text
data: {"id":"chatcmpl_...","object":"chat.completion.chunk","choices":[{"index":0,"delta":{"role":"assistant"},"finish_reason":null}]}

data: {"id":"chatcmpl_...","object":"chat.completion.chunk","choices":[{"index":0,"delta":{"content":"Hello"},"finish_reason":null}]}

data: {"id":"chatcmpl_...","object":"chat.completion.chunk","choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}

data: [DONE]
```

Requirements:

* Forward text deltas immediately.
* Do not accumulate the full response.
* Use bounded channels.
* Detect client disconnects.
* Cancel or detach the internal Codex turn safely.
* Stop sending once the connection closes.
* Send periodic SSE keepalive messages.
* Do not leak raw Codex JSON-RPC events.
* Ignore internal command, tool, diff, and status events unless required to derive the final text.

Suggested channel capacity:

```rust
mpsc::channel(32)
```

Backpressure must prevent unbounded memory growth.

---

# Responses API

Implement:

```text
POST /v1/responses
```

This should also remain purely OpenAI-compatible.

## Minimum supported request

```json
{
  "model": "codex",
  "input": "Explain Rust ownership.",
  "stream": false
}
```

Also support a basic message array:

```json
{
  "model": "codex",
  "input": [
    {
      "role": "user",
      "content": "Explain Rust ownership."
    }
  ],
  "stream": true
}
```

Implement only the subset needed for text input and text output.

Do not expose native Codex events.

Non-streaming response example:

```json
{
  "id": "resp_<uuid>",
  "object": "response",
  "status": "completed",
  "model": "codex",
  "output": [
    {
      "type": "message",
      "role": "assistant",
      "content": [
        {
          "type": "output_text",
          "text": "Rust ownership..."
        }
      ]
    }
  ]
}
```

Streaming should use OpenAI Responses-style SSE events where practical.

At minimum:

```text
response.created
response.output_text.delta
response.output_text.done
response.completed
```

Do not return Codex event names.

Document the supported subset clearly.

---

# Error Format

Use OpenAI-compatible error responses:

```json
{
  "error": {
    "message": "The requested model is not supported.",
    "type": "invalid_request_error",
    "param": "model",
    "code": "model_not_found"
  }
}
```

Map internal failures into stable categories:

```text
invalid_request_error
authentication_error
rate_limit_error
backend_unavailable
timeout_error
internal_server_error
```

Suggested HTTP mapping:

```text
400 invalid input
401 invalid gateway API key
404 unsupported endpoint or model
408 request timeout
413 request body too large
429 concurrency or upstream rate limit
502 Codex protocol failure
503 Codex unavailable or restarting
504 Codex execution timeout
```

Do not return raw child-process stderr to clients.

Log a sanitized correlation ID for debugging.

---

# Rust Design

## Recommended dependencies

```toml
[dependencies]
axum = "0.8"
tokio = { version = "1", features = ["full"] }
tokio-util = { version = "0.7", features = ["codec"] }
tower = "0.5"
tower-http = { version = "0.6", features = [
  "catch-panic",
  "cors",
  "limit",
  "request-id",
  "trace"
] }

serde = { version = "1", features = ["derive"] }
serde_json = "1"
futures = "0.3"
async-stream = "0.3"
bytes = "1"

uuid = { version = "1", features = ["v4", "serde"] }
thiserror = "2"
tracing = "0.1"
tracing-subscriber = { version = "0.3", features = ["env-filter"] }

clap = { version = "4", features = ["derive", "env"] }
async-trait = "0.1"
```

Use current stable mutually compatible versions and commit `Cargo.lock`.

---

## Suggested source layout

```text
codex-openai-gateway/
├── Cargo.toml
├── Cargo.lock
├── src/
│   ├── main.rs
│   ├── config.rs
│   ├── error.rs
│   ├── auth.rs
│   ├── state.rs
│   ├── api/
│   │   ├── mod.rs
│   │   ├── health.rs
│   │   ├── models.rs
│   │   ├── chat_completions.rs
│   │   ├── responses.rs
│   │   └── openai_types.rs
│   └── codex/
│       ├── mod.rs
│       ├── backend.rs
│       ├── app_server.rs
│       ├── exec.rs
│       ├── protocol.rs
│       ├── actor.rs
│       └── event_mapper.rs
├── tests/
│   ├── chat_completions.rs
│   ├── responses.rs
│   ├── streaming.rs
│   └── fake_codex.rs
├── Dockerfile
├── compose.yml
├── .env.example
└── README.md
```

Keep public OpenAI models separate from raw Codex protocol models.

---

# Codex Backend Abstraction

Use a shared internal abstraction:

```rust
#[async_trait]
pub trait CodexBackend: Send + Sync {
    async fn execute(
        &self,
        request: CodexRequest,
    ) -> Result<CodexRun, GatewayError>;
}
```

Suggested request:

```rust
pub struct CodexRequest {
    pub model: String,
    pub prompt: String,
    pub timeout: Duration,
}
```

Suggested run handle:

```rust
pub struct CodexRun {
    pub events: mpsc::Receiver<Result<CodexEvent, GatewayError>>,
    pub cancel: CancellationToken,
}
```

Suggested normalized event enum:

```rust
pub enum CodexEvent {
    TextDelta(String),
    Completed,
    Usage {
        input_tokens: Option<u64>,
        output_tokens: Option<u64>,
    },
    Failed(String),
}
```

Do not expose this enum directly over HTTP.

---

# App-Server Actor

Run one persistent `codex app-server` process.

Use a dedicated Tokio task that owns:

* Child process.
* Stdin writer.
* Stdout reader.
* Pending request map.
* Active turn event channels.
* Initialization state.
* Restart state.

Avoid wrapping child stdin and pending maps in several broad `Arc<Mutex<_>>` values.

Suggested structure:

```rust
pub struct AppServerHandle {
    command_tx: mpsc::Sender<AppServerCommand>,
}
```

Commands:

```rust
enum AppServerCommand {
    Execute {
        request: CodexRequest,
        events_tx: mpsc::Sender<Result<CodexEvent, GatewayError>>,
        result_tx: oneshot::Sender<Result<RunMetadata, GatewayError>>,
    },
    Cancel {
        run_id: Uuid,
    },
    Shutdown,
}
```

For each API request:

1. Send `thread/start`.
2. Omit `cwd`.
3. Send `turn/start`.
4. Route text events into normalized `TextDelta` events.
5. Mark completion.
6. Release all per-request routing state.

The app-server protocol requires initialization before other requests. Perform:

1. `initialize`
2. Wait for success.
3. Send `initialized`.
4. Mark the backend ready.

---

# Process Recovery

The actor must detect:

* EOF on stdout.
* Broken stdin.
* Child exit.
* Invalid initialization response.
* Protocol parse failure.
* Request timeout.

On process failure:

* Mark readiness false.
* Fail active requests.
* Drop pending request senders.
* Restart with bounded exponential backoff.
* Re-run initialization.
* Mark readiness true after successful handshake.

Suggested defaults:

```text
initial delay: 500 ms
maximum delay: 30 seconds
maximum consecutive restart attempts: configurable
```

Do not restart in a tight loop.

---

# `codex exec` Fallback

The fallback backend is internal only.

It should:

* Run `codex exec --json -`.
* Use `/home/codex/runtime` as the current directory.
* Send the prompt through stdin.
* Parse stdout incrementally as JSONL.
* Normalize text events.
* Capture only a bounded amount of stderr for diagnostics.
* Kill the process on timeout.
* Kill it when the HTTP client disconnects.
* Never expose raw JSONL.

Fallback policy:

```text
app-server available → use app-server
app-server unavailable before request starts → optionally use exec
app-server fails mid-turn → fail request; do not replay automatically
```

Do not automatically replay a partially completed request because that could duplicate side effects.

Since v1 uses a neutral directory and should not perform project work, use conservative Codex permissions.

---

# Authentication

## Host Codex authentication

Mount:

```text
${HOME}/.codex
```

to:

```text
/home/codex/.codex
```

Set:

```text
CODEX_HOME=/home/codex/.codex
```

The host should already be authenticated through:

```bash
codex login
```

Do not:

* Copy `.codex` into the image.
* Bake tokens into an image layer.
* Print authentication data.
* Mount the entire host home directory.

The Codex CLI officially supports authentication through ChatGPT OAuth, device auth, API key, or a supplied access token.

---

## Gateway authentication

Use a separate static bearer token:

```http
Authorization: Bearer <CODEX_GATEWAY_API_KEY>
```

Environment variable:

```text
CODEX_GATEWAY_API_KEY
```

Exempt:

```text
GET /health
```

Optionally require authentication for `/ready`.

Always require authentication for `/v1/*`.

Use constant-time comparison where practical.

Never log authorization headers.

---

# Memory Efficiency

The main memory rules are:

## Stream incrementally

Do not store full streaming responses.

Use:

* Bounded Tokio channels.
* Incremental JSONL decoding.
* Incremental SSE serialization.
* `Bytes` where useful.

## Bound everything

Set limits for:

* Request body size.
* Prompt size.
* Number of messages.
* Message content length.
* Concurrent requests.
* Pending app-server requests.
* Active streams.
* Buffered stderr.
* Non-streaming response size.
* Execution timeout.

Suggested initial defaults:

```text
max request body: 1 MiB
max messages: 128
max combined prompt: 512 KiB
max concurrent Codex runs: 4
event channel capacity: 32
default timeout: 10 minutes
max stderr capture: 64 KiB
```

## Avoid transcript persistence

Do not keep completed prompts or answers in memory.

Do not implement a gateway conversation store.

Each HTTP request gets a fresh internal Codex thread.

## Minimize cloning

Avoid cloning message bodies and JSON objects unnecessarily.

However, prioritize clean typed code first. Profile before introducing complex borrowed JSON representations.

---

# Containerization

## Dockerfile

Use a multi-stage build.

Example shape:

```dockerfile
FROM rust:bookworm AS builder

WORKDIR /build

COPY Cargo.toml Cargo.lock ./
COPY src ./src
COPY tests ./tests

RUN cargo build --release --locked

FROM node:22-bookworm-slim

ARG CODEX_VERSION

RUN npm install --global "@openai/codex@${CODEX_VERSION}" \
    && npm cache clean --force

RUN mkdir -p \
    /home/codex/.codex \
    /home/codex/runtime \
    && chown -R node:node /home/codex

COPY --from=builder \
    /build/target/release/codex-openai-gateway \
    /usr/local/bin/codex-openai-gateway

USER node
WORKDIR /home/codex/runtime

ENV CODEX_HOME=/home/codex/.codex
ENV RUST_LOG=info

EXPOSE 8989

ENTRYPOINT ["codex-openai-gateway"]
```

Pin `CODEX_VERSION`.

Do not install `latest`.

Verify the exact Codex command and protocol against the pinned version during implementation.

---

## Docker Compose

```yaml
services:
  codex-gateway:
    build:
      context: .
      args:
        CODEX_VERSION: ${CODEX_VERSION}

    ports:
      - "127.0.0.1:8989:8989"

    environment:
      CODEX_HOME: /home/codex/.codex
      CODEX_GATEWAY_API_KEY: ${CODEX_GATEWAY_API_KEY}
      CODEX_DEFAULT_MODEL: ${CODEX_DEFAULT_MODEL}
      RUST_LOG: ${RUST_LOG:-info}

    volumes:
      - ${HOME}/.codex:/home/codex/.codex

    user: "${HOST_UID:-1000}:${HOST_GID:-1000}"

    restart: unless-stopped
```

Do not mount any project directory in v1.

Provide Linux startup instructions:

```bash
export HOST_UID=$(id -u)
export HOST_GID=$(id -g)
docker compose up --build
```

Document macOS Docker Desktop differences.

---

# Configuration

Support environment-based configuration for v1.

Required:

```text
CODEX_GATEWAY_API_KEY
CODEX_DEFAULT_MODEL
```

Optional:

```text
CODEX_BINARY=codex
CODEX_HOME=/home/codex/.codex
CODEX_RUNTIME_DIR=/home/codex/runtime
CODEX_EXEC_FALLBACK=true
CODEX_REQUEST_TIMEOUT_SECONDS=600
CODEX_MAX_CONCURRENT_RUNS=4
CODEX_MAX_REQUEST_BODY_BYTES=20971520
CODEX_MAX_PROMPT_BYTES=16777216
RUST_LOG=info
SERVER_HOST=0.0.0.0
SERVER_PORT=8989
```

Fail fast on invalid configuration.

Do not require a TOML configuration file for v1 unless implementation complexity clearly benefits from it.

---

# Tests

## Unit tests

Cover:

* Message-content parsing.
* Prompt serialization.
* Model alias resolution.
* OpenAI error serialization.
* Chat completion response mapping.
* SSE chunk formatting.
* Responses API formatting.
* Request-size limits.
* Authentication.
* Codex event normalization.
* App-server JSON-RPC routing.
* Timeout behavior.

## Fake Codex process

Create a test helper executable or script that behaves like app-server:

* Accepts initialization.
* Accepts thread creation.
* Accepts turn creation.
* Emits deterministic text deltas.
* Emits completion.
* Can simulate crashes.
* Can emit malformed JSON.
* Can delay responses.
* Can exit mid-stream.

Tests should not consume real Codex usage.

## Integration tests

Test:

```text
GET /health
GET /ready
GET /v1/models
POST /v1/chat/completions
POST /v1/chat/completions with stream=true
POST /v1/responses
POST /v1/responses with stream=true
```

Also test:

* Invalid API key.
* Missing model.
* Unsupported model.
* Empty messages.
* Oversized body.
* Backend unavailable.
* Backend restart.
* Client disconnect.
* Timeout.
* Concurrent requests.

## Manual smoke test

Provide:

```bash
curl http://127.0.0.1:8989/v1/chat/completions \
  -H "Authorization: Bearer $CODEX_GATEWAY_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{
    "model": "codex",
    "messages": [
      {
        "role": "user",
        "content": "Explain bounded channels in Tokio."
      }
    ]
  }'
```

Streaming:

```bash
curl -N http://127.0.0.1:8989/v1/chat/completions \
  -H "Authorization: Bearer $CODEX_GATEWAY_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{
    "model": "codex",
    "stream": true,
    "messages": [
      {
        "role": "user",
        "content": "Write a short Rust function that checks whether a number is even."
      }
    ]
  }'
```

OpenAI SDK example:

```python
from openai import OpenAI

client = OpenAI(
    base_url="http://127.0.0.1:8989/v1",
    api_key="local-gateway-key",
)

response = client.chat.completions.create(
    model="codex",
    messages=[
        {
            "role": "user",
            "content": "Explain Rust ownership in two paragraphs.",
        }
    ],
)

print(response.choices[0].message.content)
```

---

# README Requirements

The README must include:

1. Purpose.
2. Architecture.
3. Prerequisites.
4. Host Codex login.
5. Environment setup.
6. Docker Compose startup.
7. Health checks.
8. Curl examples.
9. OpenAI SDK example.
10. Streaming example.
11. Supported API subset.
12. Unsupported OpenAI fields.
13. Security model.
14. Host-auth mount behavior.
15. Troubleshooting.
16. Codex CLI version pinning.
17. How to run tests.
18. Known limitations.

Explicitly document:

* This is an OpenAI-compatible adapter, not an official OpenAI API implementation.
* Each request creates a fresh internal Codex thread.
* Client-managed conversation continuity is represented through the supplied `messages`.
* No workspace or host project is exposed.
* The gateway operates from a neutral internal directory.
* Codex may still have local tools available according to its installed configuration.
* The service should remain bound to localhost unless deliberately secured.

---

# Suggested Skills

The coding agent should invoke or apply the following skills where available.

## Rust service architecture

Use a Rust backend or service-design skill for:

* Axum routing.
* Tokio process management.
* Actor patterns.
* Graceful shutdown.
* Structured error handling.
* Backpressure.
* Low-allocation streaming.

## OpenAI API compatibility

Use an OpenAI API or protocol skill for:

* Chat Completions request and response formats.
* Chat Completions SSE chunks.
* Responses API text formats.
* OpenAI-style error envelopes.
* Compatibility testing with official SDKs.

Verify current API shapes against official OpenAI documentation rather than relying solely on memory.

## Codex app-server protocol

Use a Codex CLI or app-server skill for:

* Initialization handshake.
* `thread/start`.
* `turn/start`.
* Event schemas.
* Cancellation.
* Authentication checks.
* Version-specific protocol details.

Generate protocol schemas from the pinned Codex CLI when possible rather than manually guessing every event.

## Docker and container security

Use a Docker or container-hardening skill for:

* Multi-stage builds.
* Non-root runtime.
* Bind-mounted authentication.
* UID/GID handling.
* Signal forwarding.
* Minimal runtime image.
* Secret-safe logs.

## Rust testing

Use a Rust testing or integration-testing skill for:

* Fake child processes.
* Tokio async tests.
* SSE testing.
* Crash recovery tests.
* Timeout tests.
* Client disconnect tests.

## Security review

Use a security-review skill before completion.

Review:

* Bearer-token validation.
* Host credential mounting.
* Process invocation.
* Environment leakage.
* Log redaction.
* Request limits.
* Concurrency limits.
* Public binding risks.
* Command injection risks.
* Untrusted prompt handling.

## API contract testing

Use an API testing skill to validate the gateway with:

* `curl`.
* Official OpenAI Python SDK.
* Official OpenAI JavaScript or TypeScript SDK.
* Streaming and non-streaming calls.
* Invalid-input scenarios.

---

# Implementation Order

Execute in this order:

1. Scaffold the Rust project.
2. Define OpenAI-compatible request, response and error types.
3. Implement configuration and gateway bearer authentication.
4. Implement the fake Codex backend.
5. Implement `/health`, `/ready`, and `/v1/models`.
6. Implement non-streaming `/v1/chat/completions`.
7. Implement streaming `/v1/chat/completions`.
8. Implement the app-server protocol actor.
9. Integrate app-server with Chat Completions.
10. Implement the minimal `/v1/responses` subset.
11. Implement optional `codex exec` fallback.
12. Add process restart and graceful shutdown.
13. Add Dockerfile and Compose.
14. Add full automated tests.
15. Test using the host Codex authentication mount.
16. Test with official OpenAI SDKs.
17. Run formatting, linting and security review.
18. Complete the README.

---

# Acceptance Criteria

The work is complete when:

* `docker compose up --build` starts successfully.
* The container uses the host-mounted Codex authentication.
* No API key needs to be copied into the image.
* No project or workspace mount is required.
* `/ready` reflects actual app-server readiness.
* `/v1/models` returns the configured model aliases.
* Non-streaming Chat Completions work.
* Streaming Chat Completions work through standard SSE.
* The minimal Responses API works.
* Official OpenAI SDKs can call the gateway by changing `base_url`.
* The HTTP API exposes no Codex-native endpoints.
* The HTTP API accepts no workspace or `cwd`.
* Streaming uses bounded memory.
* Client disconnects do not leave uncontrolled active jobs.
* App-server crashes are detected and recovered.
* Tests run without real Codex usage through a fake backend.
* Real Codex smoke tests are documented.
* The gateway binds to localhost by default in Compose.
* Logs contain no credentials or authorization headers.
* `cargo fmt --check` passes.
* `cargo clippy --all-targets --all-features -- -D warnings` passes.
* `cargo test --all` passes.
* The README documents every manual setup step.
