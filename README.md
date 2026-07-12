# Codex OpenAI Gateway

An OpenAI-compatible Rust adapter backed by the locally authenticated Codex CLI. It exposes only `/health`, `/ready`, `/v1/models`, `/v1/chat/completions`, and `/v1/responses`; it is not an official OpenAI API implementation.

Each request creates a fresh ephemeral internal Codex thread. Conversation continuity is supplied by the caller's `messages` or `input`; no workspace, `cwd`, project mount, thread ID, or Codex-native endpoint is public. Codex runs in `/home/codex/runtime` and may still have local tools available according to its installed configuration.

The app-server transport is newline-delimited JSON over stdio. The gateway performs the required `initialize`/`initialized` handshake, then starts a thread and turn for each request. It forwards only agent-message text deltas and completion state; tool, command, approval, diff, and native event messages never appear in the HTTP response. Turns use the gateway's conservative read-only, no-network tool policy; clients cannot override it.

## Prerequisites

Install Rust, Docker/Compose, and a pinned Codex CLI version. Authenticate the host CLI with `codex login`. Do not copy credentials into this image.

Create `.env` from `.env.example`, set `CODEX_DEFAULT_MODEL` and `CODEX_VERSION`, then start:

```bash
export HOST_UID=$(id -u)
export HOST_GID=$(id -g)
docker compose up --build
```

Compose binds `${HOME}/.codex` read/write to `/home/codex/.codex` and defaults the service to localhost. On macOS, allow the home directory in Docker Desktop file sharing.

On Linux, Compose also mounts the host CA bundle into the container. This lets Codex validate upstream TLS certificates when the host trust store has certificates newer than the base image or includes a network-specific root. Set `HOST_CA_BUNDLE` in `.env` if your bundle is elsewhere.

### External Docker network

The Compose service joins an existing external Docker network named by `DOCKER_NETWORK` (default: `homelab`) and publishes port `8989` on all host interfaces. Create the network once if it does not already exist:

```bash
docker network create homelab
```

Set `DOCKER_NETWORK` in `.env` to your existing network name, then start the gateway. Other containers on that network can call `http://codex-gateway:8989/v1`; the host can call `http://127.0.0.1:8989/v1`; LAN clients can call `http://<host-lan-ip>:8989/v1`.

This does not configure Tailscale Serve or Funnel and does not expose a public endpoint. The gateway has no authentication, so restrict LAN access with a firewall or trusted-network policy before using it outside a fully trusted network.

## API examples

```bash
curl http://127.0.0.1:8989/v1/chat/completions -H 'Content-Type: application/json' -d '{"model":"codex","messages":[{"role":"user","content":"Explain bounded Tokio channels."}]}'
```

For streaming add `"stream":true` and use `curl -N`; the gateway emits standard chat SSE chunks and `[DONE]`. `/v1/responses` accepts `{ "model":"codex", "input":"Hello" }` and emits the minimal Responses text format.

The repository also includes a dependency-free `uv` client:

```bash
uv run scripts/codex_client.py "Explain bounded Tokio channels."
uv run scripts/codex_client.py --stream "Write a short haiku about containers."
uv run scripts/codex_client.py --responses "Explain Rust ownership."
```

The supported subset is text messages, roles `system`, `developer`, `user`, and `assistant`, model alias `codex`, `stream`, and common harmless generation fields. Image/audio/file content and unsupported models are rejected. Usage is omitted when unavailable.

OpenAI Python SDK usage only changes the base URL:

```python
from openai import OpenAI

client = OpenAI(base_url="http://127.0.0.1:8989/v1", api_key="local-only")
answer = client.chat.completions.create(
    model="codex",
    messages=[{"role": "user", "content": "Explain Rust ownership in two paragraphs."}],
)
print(answer.choices[0].message.content)
```

This local-only build has no gateway authentication; `/health`, `/ready`, and `/v1/*` are public to anything that can reach the listener. Credentials, authorization headers, paths, and child stderr are not returned to clients. Keep the listener on localhost and do not expose it to an untrusted network.

## Configuration and operations

Required for Compose: `CODEX_DEFAULT_MODEL` and pinned `CODEX_VERSION`. Optional settings include `CODEX_BINARY`, `CODEX_HOME`, `CODEX_RUNTIME_DIR`, `CODEX_EXEC_FALLBACK`, request timeout, concurrency, body/prompt/response limits, `SERVER_HOST`, `SERVER_PORT`, and `RUST_LOG`. The app-server is the primary backend; `codex exec --json -` is an internal fallback when startup is unavailable. Concurrency saturation returns HTTP 429 instead of retaining unbounded queued requests.

Run checks with `cargo fmt --check`, `cargo clippy --all-targets --all-features -- -D warnings`, and `cargo test --all`. Real Codex smoke tests require valid host authentication; automated tests must use a fake Codex executable and never consume real usage.

## Troubleshooting

If `/ready` returns `503`, inspect the container logs for a Codex startup or authentication problem and confirm that the host's `${HOME}/.codex` directory is mounted. The gateway does not print credentials or child stderr in HTTP responses. To verify a pinned CLI's exact app-server schema, run `codex app-server generate-json-schema --out /tmp/codex-schema` with that same CLI version.

Known limitations: this v1 adapter does not persist conversations, expose project files, provide accurate token usage, or replay failed turns. It supports a minimal text-only Responses API subset. Streaming request deadlines and disconnected clients cooperatively interrupt the active app-server turn; an interrupted or failed Codex turn is returned as an error rather than a successful completion.
