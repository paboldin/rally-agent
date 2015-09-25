#!/usr/bin/python

import argparse
import datetime
import os
import subprocess
import sys
import tempfile
import threading
import uuid
import zmq

class CommandExecutor(object):
    def __init__(self, req, resp):
        self.req = req
        self.resp = resp
        self.thread = req.get("thread")
        self.stdout_fh = self.stderr_fh = None
        self.child_stdout_fh = self.child_stderr_fh = None

    def _thread_target(self, process):
        self.exit_code = process.wait()

    @classmethod
    def _get_redirection(cls, config, thread=False, is_stderr=False):
        if config == "null":
            return open("/dev/null", "wb")
        elif config == "stdout" and is_stderr:
            return subprocess.STDOUT
        elif config == "tmpfile" or thread:
            return tempfile.NamedTemporaryFile()
        else:
            return subprocess.PIPE

    def _get_stdout_stderr(self):
        stdout_fh = self._get_redirection(
            self.req.get("stdout", "").lower(),
            thread=self.thread)

        stderr_fh = self._get_redirection(
            self.req.get("stderr", "").lower(),
            thread=self.thread, is_stderr=True)

        return stdout_fh, stderr_fh

    def run(self):
        req = self.req
        resp = self.resp

        stdout_fh, stderr_fh = self._get_stdout_stderr()

        process = subprocess.Popen(
            req["path"], stdout=stdout_fh, stderr=stderr_fh
        )
        stdout = stderr = None

        if not self.thread:
            stdout, stderr = process.communicate()
            resp["exit_code"] = process.wait()
        else:

            if stdout_fh not in (subprocess.PIPE, subprocess.STDOUT):
                self.child_stdout_fh = stdout_fh
                self.stdout_fh = open(stdout_fh.name, "rb")
            if stderr_fh not in (subprocess.PIPE, subprocess.STDOUT):
                self.child_stderr_fh = stderr_fh
                self.stderr_fh = open(stderr_fh.name, "rb")

            self.thread = threading.Thread(
                target=self._thread_target, args=(process,))
            self.thread.start()

        if stdout:
            resp["stdout"] = stdout.decode("utf-8")
        elif hasattr(stdout_fh, "name"):
            resp["stdout_fh"] = stdout_fh.name
        if stderr:
            resp["stderr"] = stderr.decode("utf-8")
        elif hasattr(stderr_fh, "name"):
            resp["stderr_fh"] = stderr_fh.name
        return resp

    def clear(self):
        for fh in (self.child_stdout_fh, self.child_stderr_fh, self.stdout_fh,
                   self.stderr_fh):
            if fh is not None:
                fh.close()


class Agent(object):
    def __init__(self, subscribe_url, push_url, agent_id=None):
        if agent_id is None:
            agent_id = str(uuid.uuid4())
        self.agent_id = agent_id
        self.executor = None

        self.subscribe_socket = self.init_subscribe_zmq(subscribe_url)
        self.push_socket = self.init_push_zmq(push_url)

    def init_subscribe_zmq(self, subscribe_url):
        subscribe_context = zmq.Context()
        subscribe_socket = subscribe_context.socket(zmq.SUB)
        subscribe_socket.connect(subscribe_url)
        subscribe_socket.setsockopt_string(zmq.SUBSCRIBE, u"")

        return subscribe_socket

    def init_push_zmq(self, push_url):
        push_context = zmq.Context()
        push_socket = push_context.socket(zmq.PUSH)
        push_socket.connect(push_url)

        return push_socket

    def recv_request(self):
        request = self.subscribe_socket.recv_json()
        target = request.get("target")
        if target and target != self.agent_id and self.agent_id not in target:
            return

        return request

    def do_default(sef, req, resp):
        raise ValueError(
            "Action '%s' unknown." % req.get("action", "unspecified"))

    def loop(self):
        req = self.recv_request()
        if req is None:
            return
        resp = {
            "req": req["req"],
            "agent": self.agent_id
        }
        handler = getattr(self, "do_%s" % req.get("action", "default"),
                          self.do_default)
        try:
            new_resp = handler(req, resp)
            if new_resp: resp = new_resp
        except Exception as e:
            resp["error"] = str(e)
        self.push_socket.send_json(resp)

    def do_ping(self, req, resp):
        resp["time"] = datetime.datetime.utcnow().isoformat()

    def do_tail(self, req, resp):
        if not self.executor or not (
                self.executor.stdout_fh or
                self.executor.stderr_fh):
            raise ValueError("No executor or pipes.")

        size = int(req.get("size", -1))
        if self.executor.stdout_fh:
            resp["stdout"] = self.executor.stdout_fh.read(size).decode("utf-8")
        if self.executor.stderr_fh:
            resp["stderr"] = self.executor.stderr_fh.read(size).decode("utf-8")

    def do_check(self, req, resp):
        if not self.executor or not self.executor.thread:
            raise ValueError("No executor.")

        if req.get("wait") or req.get("clear"):
            self.executor.thread.join()
        resp["exit_code"] = getattr(self.executor, "exit_code", None)
        if req.get("clear"):
            self.executor.clear()
            self.executor = None

    def do_command(self, req, resp):
        if self.executor and self.executor.thread:
            raise ValueError("A command is already being executed.")

        executor = CommandExecutor(req, resp)
        if executor.thread:
            self.executor = executor
        return executor.run()


def parse_args(args=None):
    parser = argparse.ArgumentParser(description="Run a ZMQ agent")

    parser.add_argument(
        "--subscribe-url", help="ZMQ Subscribe bind URL",
        default="tcp://localhost:1234")
    parser.add_argument(
        "--push-url", help="ZMQ Push bind URL",
        default="tcp://localhost:1235")
    parser.add_argument(
        "--agent-id", help="ZMQ agent ID")

    return parser.parse_args(args)

def main(args=None):
    args = parse_args(args)
    agent = Agent(args.subscribe_url, args.push_url, args.agent_id)
    while True:
        agent.loop()

if __name__ == "__main__":
    main()
