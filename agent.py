#!/usr/bin/python

import argparse
import datetime
import subprocess
import sys
import tempfile
import uuid
import zmq

class Agent(object):
    def __init__(self, subscribe_url, push_url, agent_id=None):
        if agent_id is None:
            agent_id = str(uuid.uuid4())
        self.agent_id = agent_id
        self.init_zmq(subscribe_url, push_url)

    def init_zmq(self, subscribe_url, push_url):
        subscribe_context = zmq.Context()
        subscribe_socket = subscribe_context.socket(zmq.SUB)
        subscribe_socket.connect(subscribe_url)
        subscribe_socket.setsockopt_string(zmq.SUBSCRIBE, u"")

        push_context = zmq.Context()
        push_socket = push_context.socket(zmq.PUSH)
        push_socket.connect(push_url)

        self.subscribe_socket = subscribe_socket
        self.push_socket = push_socket

    def recv_request(self):
        request = self.subscribe_socket.recv_json()
        target = request.get("target")
        if target and target != self.agent_id and self.agent_id not in target:
            return

        return request

    def loop(self):
        req = self.recv_request()
        action = req.get("action")
        resp = {
            "req": req["req"],
            "agent": self.agent_id
        }
        handler = getattr(self, "do_%s" % action, None)
        if not handler:
            r["error"] = "Action %s unknown" % action

        resp = handler(resp)
        self.push_socket.send_json(resp)

    def do_ping(self, resp):
        resp["time"] = datetime.datetime.utcnow().isoformat()
        return resp

    def do_command(self, resp):
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
            resp["stdout"] = stdout
        else:
            resp["stdout_fh"] = stdout_fh.name
        if stderr is not None:
            resp["stderr"] = stderr
        else:
            resp["stderr_fh"] = stderr_fh.name

    def do_read(self, resp):
        with file(resp["filename"]) as fh:
            fh.seek(int(resp.get("seek", 0)))
            if "size" in resp:
                content = fh.read(int(resp["size"]))
            else:
                content = fh.read()
            resp["content"] = content


def parse_args():
    parser = argparse.ArgumentParser(description="Run a ZMQ agent")

    parser.add_argument(
        "--subscribe-url", help="ZMQ Subscribe bind URL",
        default="tcp://localhost:1234")
    parser.add_argument(
        "--push-url", help="ZMQ Push bind URL",
        default="tcp://localhost:1235")
    parser.add_argument(
        "--agent-id", help="ZMQ agent ID")

    return parser.parse_args()

def main():
    args = parse_args()
    agent = Agent(args.subscribe_url, args.push_url, args.agent_id)
    while True:
        agent.loop()

if __name__ == "__main__":
    main()
