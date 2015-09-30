#!/usr/bin/python

import itertools
import multiprocessing
import random
import requests
import six
import subprocess
import tempfile
import threading
import time
import unittest
import zmq

import agent
import masteragent


class SwarmTestCase(unittest.TestCase):

    @classmethod
    def _ping(cls, timeout=100, agents=1000.):
        try:
            r = requests.get("%s/ping?timeout=%d&agents=%d" % (
                cls.http_url, timeout, agents
            ))
            return r.json()
        except (requests.exceptions.RequestException, ValueError):
            return []

    @classmethod
    def _try_start_masteragent(cls):
        http_host = "127.0.0.1"
        http_port = random.randint(10000, 11000)
        publish_url = "tcp://127.0.0.1:%d" % random.randint(11000, 12000)
        pull_url = "tcp://127.0.0.1:%d" % random.randint(12000, 13000)

        masteragent_process = multiprocessing.Process(
            target=masteragent.main,
            args=(("--http-host", http_host, "--http-port", str(http_port),
                  "--publish-url", publish_url, "--pull-url", pull_url),)
        )
        masteragent_process.start()

        http_url = "http://%s:%d" % (http_host, http_port)
        for i in range(10):
            try:
                r = requests.get("%s/ping?timeout=1&agents=0" % http_url)
                r.json()
            except (requests.exceptions.RequestException, ValueError):
                time.sleep(0.1)
                continue
            return masteragent_process, http_url, publish_url, pull_url
        raise EnvironmentError()

    @classmethod
    def _start_masteragent(cls):
        for i in range(3):
            try:
                return cls._try_start_masteragent()
            except EnvironmentError:
                pass

    @classmethod
    def _try_start_agent(cls, agent_id="0"):
        agent_process = multiprocessing.Process(
            target=agent.main,
            args=(("--subscribe-url", cls.publish_url,
                   "--push-url", cls.pull_url,
                   "--agent-id", agent_id),)
        )
        agent_process.start()

        for i in range(10):
            for online in cls._ping():
                if online["agent"] == agent_id:
                    return agent_process
            time.sleep(0.1)
        raise EnvironmentError()

    AGENT_IDS = itertools.count()

    @classmethod
    def _start_agent(cls, agent_id=None):
        if agent_id is None:
            agent_id = str(cls.AGENT_IDS.next())
        for i in range(3):
            try:
                return cls._try_start_agent(agent_id)
            except EnvironmentError:
                continue

    @classmethod
    def setUpClass(cls):
        (cls.masteragent, cls.http_url,
         cls.publish_url, cls.pull_url) = cls._start_masteragent()
        cls.agents = []
        cls.agents.append(cls._start_agent())
        cls.agents.append(cls._start_agent())

    @classmethod
    def tearDownClass(cls):
        cls.masteragent.terminate()
        for runnning_agent in cls.agents:
            runnning_agent.terminate()

    def test_ping(self):
        p = self._ping()
        req_id = p[0]["req"]
        self.assertTrue(all(req_id == ap["req"] for ap in p))
        self.assertEqual(
            ["0", "1"],
            list(sorted(ap["agent"] for ap in p)))

    def test_ping_missed(self):
        p = self._ping(agents=1)
        self.assertEqual(1, len(p))
        p = p[0]

        m = requests.get("%s/missed?timeout=100" % self.http_url).json()

        self.assertIn(p["req"], m["missed"])
        missed = m["missed"][p["req"]]
        self.assertEqual(1, len(missed))

        self.assertEqual(
            ["1", "0"][int(p["agent"])],
            missed[0]["agent"])

    def test_command_poll(self):
        requests.post("%s/command?agents=0" % self.http_url,
            data={
                "path": ["bash", "--version"]
            }
        )

        polls = requests.get("%s/poll?agents=1" % self.http_url).json()
        self.assertEqual(1, len(polls))
        polls.extend(
            requests.get(
                "%s/poll?agents=1&req=%s" % (self.http_url, polls[0]["req"])
            ).json()
        )
        self.assertEqual(2, len(polls))

        agents = list(sorted([poll["agent"] for poll in polls]))
        self.assertEqual(["0", "1"], agents)

        bash_version = subprocess.check_output(["bash", "--version"])

        for poll in polls:
            self.assertEqual(0, poll["exit_code"])
            self.assertEqual(bash_version, poll["stdout"])

    def _tail_and_wait(self, agents=2):
        file_contents = ["" for i in range(agents)]

        while True:
            tails = requests.post(
                "%s/tail?agents=%d" % (self.http_url, agents),
                data={
                    "size": 16
                }
            ).json()

            updated = False
            for i in range(agents):
                if tails[i]["stdout"]:
                    updated = True
                    file_contents[i] += tails[i]["stdout"]

            if not updated:
                checks = requests.post(
                    "%s/check?agents=%d" % (self.http_url, agents)
                ).json()

                finished = 0
                for check in checks:
                    if check["exit_code"] is not None:
                        finished += 1
                if finished == agents:
                    break

        return file_contents

    def test_commandthread_tail_check(self):
        tails = requests.post("%s/tail?agents=2" % self.http_url).json()
        self.assertEqual(2, len(tails))
        for tail in tails:
            self.assertEqual("No executor or pipes.", tail["error"])

        commands = requests.post("%s/command?agents=2" % self.http_url,
            data={
                "path": ["bash", "--version"],
                "thread": "true"
            }
        ).json()
        self.assertEqual(2, len(commands))


        bash_version = subprocess.check_output(["bash", "--version"])

        file_contents = self._tail_and_wait(2)
        for file_content in file_contents:
            self.assertEqual(bash_version, file_content)

        commands = requests.post("%s/command?agents=2" % self.http_url,
            data={
                "path": ["bash", "--version"],
                "thread": "true"
            }
        ).json()
        for command in commands:
            self.assertEqual("A command is already being executed.",
                             command["error"])

        checks = requests.post(
            "%s/check?agents=2" % self.http_url,
            {
                "clear": "true"
            }
        ).json()

    def test_commandenv(self):
        environ = ["C=D", "E=F=G", "A=B"]
        commands = requests.post(
            "%s/command?agents=2" % self.http_url,
            data={
                "path": ["env"],
                "env": environ
            }
        ).json()
        env_output = subprocess.check_output(
            ["env"],
            env=dict(var.split("=", 1) for var in environ))
        for command in commands:
            self.assertEqual(env_output, command["stdout"])
