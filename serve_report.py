"""Serve only the customer report over loopback for remote VS Code previews."""

from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlsplit


REPORT = Path(__file__).resolve().with_name("customer-report.html")
PORT = 18764


class ReportHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if urlsplit(self.path).path not in ("/", "/customer-report.html"):
            self.send_error(404)
            return
        try:
            content = REPORT.read_bytes()
        except OSError as error:
            self.log_error("Cannot read report: %s", error)
            self.send_error(500, "Cannot read customer report")
            return
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(content)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(content)


if __name__ == "__main__":
    with ThreadingHTTPServer(("127.0.0.1", PORT), ReportHandler) as server:
        print(
            f"Report: http://127.0.0.1:{PORT}/customer-report.html",
            flush=True,
        )
        try:
            server.serve_forever()
        except KeyboardInterrupt:
            pass
