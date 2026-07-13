#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# ///
"""Small, dependency-free client for the local Codex OpenAI gateway.

Examples:
  uv run scripts/codex_client.py "Explain Rust ownership."
  uv run scripts/codex_client.py --stream "Write a haiku about containers."
  uv run scripts/codex_client.py --responses "Summarize bounded channels."
"""

from __future__ import annotations

import argparse
import base64
import json
import mimetypes
import sys
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Call a local Codex OpenAI gateway.")
    parser.add_argument("prompt", nargs="?", help="Prompt text. Reads stdin when omitted.")
    parser.add_argument("--base-url", default="http://127.0.0.1:8989/v1")
    parser.add_argument("--model", default="codex")
    parser.add_argument("--stream", action="store_true", help="Print SSE text as it arrives.")
    parser.add_argument("--responses", action="store_true", help="Use /v1/responses instead of chat completions.")
    parser.add_argument("--image", action="append", default=[], metavar="PATH", help="Attach a local image; may be repeated.")
    return parser.parse_args()


def read_prompt(value: str | None) -> str:
    prompt = value if value is not None else sys.stdin.read()
    if not prompt.strip():
        raise SystemExit("A prompt argument or stdin input is required.")
    return prompt


def request_json(url: str, payload: dict[str, Any]):
    request = Request(
        url,
        data=json.dumps(payload).encode(),
        headers={"Content-Type": "application/json", "Accept": "application/json"},
        method="POST",
    )
    try:
        return urlopen(request, timeout=620)
    except HTTPError as error:
        body = error.read().decode(errors="replace")
        raise SystemExit(f"Gateway returned HTTP {error.code}: {body}") from error
    except URLError as error:
        raise SystemExit(f"Could not reach the gateway: {error.reason}") from error


def image_data_url(value: str) -> str:
    path = Path(value)
    mime_type = mimetypes.guess_type(path.name)[0]
    if mime_type is None or not mime_type.startswith("image/"):
        raise SystemExit(f"Could not infer an image MIME type from: {path}")
    try:
        encoded = base64.b64encode(path.read_bytes()).decode()
    except OSError as error:
        raise SystemExit(f"Could not read image {path}: {error}") from error
    return f"data:{mime_type};base64,{encoded}"


def print_chat_stream(response) -> None:
    for raw_line in response:
        line = raw_line.decode().strip()
        if not line.startswith("data: "):
            continue
        data = line.removeprefix("data: ")
        if data == "[DONE]":
            print()
            return
        event = json.loads(data)
        if "error" in event:
            raise SystemExit(event["error"]["message"])
        print(event["choices"][0]["delta"].get("content", ""), end="", flush=True)


def print_responses_stream(response) -> None:
    for raw_line in response:
        line = raw_line.decode().strip()
        if not line.startswith("data: "):
            continue
        event = json.loads(line.removeprefix("data: "))
        if "error" in event:
            raise SystemExit(event["error"]["message"])
        if event.get("type") == "response.output_text.delta":
            print(event["delta"], end="", flush=True)
        if event.get("type") == "response.completed":
            print()
            return


def main() -> None:
    args = parse_args()
    prompt = read_prompt(args.prompt)
    base_url = args.base_url.rstrip("/")
    images = [image_data_url(path) for path in args.image]
    if args.responses:
        input_value: Any = prompt
        if images:
            content = [{"type": "input_text", "text": prompt}]
            content.extend({"type": "input_image", "image_url": image} for image in images)
            input_value = [{"role": "user", "content": content}]
        payload = {"model": args.model, "input": input_value, "stream": args.stream}
        with request_json(f"{base_url}/responses", payload) as response:
            if args.stream:
                print_responses_stream(response)
            else:
                body = json.load(response)
                print(body["output"][0]["content"][0]["text"])
    else:
        content: Any = prompt
        if images:
            content = [{"type": "text", "text": prompt}]
            content.extend({"type": "image_url", "image_url": {"url": image}} for image in images)
        payload = {
            "model": args.model,
            "messages": [{"role": "user", "content": content}],
            "stream": args.stream,
        }
        with request_json(f"{base_url}/chat/completions", payload) as response:
            if args.stream:
                print_chat_stream(response)
            else:
                body = json.load(response)
                print(body["choices"][0]["message"]["content"])


if __name__ == "__main__":
    main()
