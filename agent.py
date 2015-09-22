#!/usr/bin/python

import time
import sys
import zmq
import subprocess
import uuid

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
        r = {
            "req": d["req"],
            "agent": AGENT
        }
        action = d.get("action")
        if action == "ping":
            pass
        elif action == "command":
            stdout, stderr = subprocess.Popen(
                " ".join(d["path"]), shell=True,
                stdout=subprocess.PIPE, stderr=subprocess.PIPE
            ).communicate()
            d["stdout"] = stdout
            d["stderr"] = stderr
        else:
            d["error"] = "Action %s unknown" % action

        push_socket.send_json(d)
