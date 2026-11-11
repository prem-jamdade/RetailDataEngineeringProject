"""Lightweight REST API simulator serving online sales JSON payloads via HTTP."""

from __future__ import annotations

import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

from config.settings import get_pipeline_config


class SalesAPIHandler(BaseHTTPRequestHandler):
    """HTTP request handler serving `/health` and `/api/v1/sales` endpoints."""

    api_feed_dir: Path = get_pipeline_config().api_feed_dir

    def log_message(self, format: str, *args) -> None:  # noqa: A002
        return  # Suppress noisy stdout access logs during automated runs

    def _send_json(self, status_code: int, payload: dict) -> None:
        body = json.dumps(payload).encode("utf-8")
        self.send_response(status_code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        if parsed.path == "/health":
            self._send_json(200, {"status": "ok"})
            return

        if parsed.path != "/api/v1/sales":
            self._send_json(404, {"error": "Endpoint not found"})
            return

        query = parse_qs(parsed.query)
        target_date = query.get("date", [None])[0]

        files = sorted(self.api_feed_dir.glob("online_sales_*.json"))
        if target_date:
            files = [f for f in files if target_date in f.name]

        batches = []
        for file_path in files:
            content = json.loads(file_path.read_text(encoding="utf-8"))
            batches.append({
                "batch_name": file_path.name,
                "date": content.get("date"),
                "records": content.get("records", []),
            })

        self._send_json(200, {"count": len(batches), "batches": batches})


def start_api_server_in_thread(
    host: str = "127.0.0.1",
    port: int = 8765,
    api_feed_dir: Path | None = None,
) -> tuple[ThreadingHTTPServer, threading.Thread]:
    """Start the local Sales REST API server in a daemon thread."""
    if api_feed_dir is not None:
        SalesAPIHandler.api_feed_dir = api_feed_dir
    server = ThreadingHTTPServer((host, port), SalesAPIHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server, thread


if __name__ == "__main__":
    cfg = get_pipeline_config()
    SalesAPIHandler.api_feed_dir = cfg.api_feed_dir
    srv = ThreadingHTTPServer(("127.0.0.1", 8765), SalesAPIHandler)
    print("Sales API Simulator listening on http://127.0.0.1:8765/api/v1/sales")
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        srv.shutdown()
