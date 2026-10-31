"""Command-backed RAP fixture whose callback dies with its process group."""
import http.server
import json
import pathlib
import sys
import threading
import time
import urllib.request


class Handler(http.server.BaseHTTPRequestHandler):
    def log_message(self, *_args):
        pass

    def reply(self, payload):
        data = json.dumps(payload).encode()
        self.send_response(200)
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self):
        self.reply({"name": "delayed", "endpoint": f"http://127.0.0.1:{self.server.server_port}/invoke",
                    "tools": [{"name": "delayed_work", "description": "Work for three seconds",
                               "inputSchema": {"type": "object"}}]})

    def do_POST(self):
        invocation = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
        self.reply({})
        if self.path != "/invoke":
            return
        pathlib.Path(sys.argv[1]).write_text(str(self.server.server_port))

        def callback():
            time.sleep(3)
            result = {"type": "tool_result", "group_id": invocation["group_id"],
                      "id": invocation["id"], "call_id": invocation.get("call_id"),
                      "text": "Delayed work complete"}
            request = urllib.request.Request(invocation["callback_url"], json.dumps(result).encode(),
                                             {"Content-Type": "application/json"})
            with urllib.request.urlopen(request, timeout=3) as response:
                response.read()

        threading.Thread(target=callback, daemon=True).start()


server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
print(json.dumps({"port": server.server_port}), flush=True)
server.serve_forever()
