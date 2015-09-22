#!/usr/bin/python

import sys
import zmq
import datetime
import subprocess
import uuid
import tempfile

subscribe_context = zmq.Context()
subscribe_socket = subscribe_context.socket(zmq.SUB)
subscribe_socket.connect("tcp://127.0.0.1:1234")
subscribe_socket.setsockopt(zmq.SUBSCRIBE, "")

push_context = zmq.Context()
push_socket = push_context.socket(zmq.PUSH)
push_socket.connect("tcp://127.0.0.1:1235")

AGENT = str(uuid.uuid4())
while True:
        d = subscribe_socket.recv_json()
        if d.get("target", AGENT) != AGENT:
            continue
        r = {
            "req": d["req"],
            "agent": AGENT
        }
        action = d.get("action")
        if action == "ping":
            r["time"] = datetime.datetime.utcnow().isoformat()
        elif action == "command":
            stdout_fh = subprocess.PIPE
            stderr_fh = subprocess.PIPE
            if d.get("stdout_log"):
                stdout_fh = tempfile.NamedTemporaryFile(delete=False)
            if d.get("stderr_log"):
                stderr_fh = tempfile.NamedTemporaryFile(delete=False)
            stdout, stderr = subprocess.Popen(
                " ".join(d["path"]),
                shell=True,
                stdout=stdout_fh,
                stderr=stderr_fh
            ).communicate()
            if stdout is not None:
                r["stdout"] = stdout
            else:
                r["stdout_fh"] = stdout_fh.name
            if stderr is not None:
                r["stderr"] = stderr
            else:
                r["stderr_fh"] = stderr_fh.name
        elif action == "read":
            with file(d["filename"]) as fh:
                fh.seek(int(d.get("seek", 0)))
                if "size" in d:
                    content = fh.read(int(d["size"]))
                else:
                    content = fh.read()
                r["content"] = content
        else:
            r["error"] = "Action %s unknown" % action

        push_socket.send_json(r)
