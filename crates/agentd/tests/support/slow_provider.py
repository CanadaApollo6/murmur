"""Credential-free Infinity provider for the ignored live-daemon test."""
import json
import os
import socket
import sys
import threading

path = sys.argv[1]
listener = socket.socket(socket.AF_UNIX)
listener.bind(path)
listener.listen()
print(path, flush=True)


def serve(connection):
    with connection, connection.makefile("rwb") as stream:
        request = json.loads(stream.readline())

        def emit(message):
            stream.write(json.dumps(message).encode() + b"\n")
            stream.flush()

        if request == "ListModels":
            emit({"Models": [{
                "model_id": "echo", "display_name": "Fake echo", "context_window": 32768,
                "max_output_tokens": 128, "supports_image_input": False,
            }]})
        else:
            assert "InvokeModel" in request, request
            with open(os.path.join(os.path.dirname(path), "requests.jsonl"), "a") as log:
                log.write(json.dumps(request) + "\n")
            emit("InvokeStarted"); import time; time.sleep(6)
            emit({"Chunk": {"Ok": {"Text": "live fake provider reply"}}})
            emit({"Chunk": {"Ok": {"Final": {"usage": None}}}})
            emit("StreamEnd")


while True:
    connection, _ = listener.accept()
    threading.Thread(target=serve, args=(connection,), daemon=True).start()
