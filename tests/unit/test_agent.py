#!/usr/bin/python

import ddt
import mock
import subprocess
import unittest
import zmq

import agent

@ddt.ddt
class CommandExecutorTestCase(unittest.TestCase):
    def test__thread_target(self):
        mock_process = mock.Mock(**{"wait.return_value": "foobar"})
        executor = agent.CommandExecutor({}, {})

        executor._thread_target(mock_process)

        self.assertEqual("foobar", executor.exit_code)
        mock_process.wait.assert_called_once_with()

    @ddt.data({"config": "null",
               "expected": "null"},
              {"config": "",
               "expected": subprocess.PIPE},
              {"config": "tmpfile",
               "expected": "namedtemp"},
              {"config": "stdout", "is_stderr": True,
               "expected": subprocess.STDOUT},
              {"config": "", "thread": True,
               "expected": "namedtemp"},
              {"config": "null", "thread": True,
               "expected": "null"},
    )
    @mock.patch("agent.open", return_value="null")
    @mock.patch("tempfile.NamedTemporaryFile", return_value="namedtemp")
    def test__get_redirection(self, param, mock_tempfile_named_temporary_file,
                              mock_agent_open):
        expected = param.pop("expected")

        actual = agent.CommandExecutor._get_redirection(**param)

        self.assertEqual(expected, actual)
        if expected == "null":
            mock_agent_open.assert_called_once_with("/dev/null", "wb")
        elif expected == "namedtemp":
            mock_tempfile_named_temporary_file.assert_called_once_with()

    def test__get_stdout_stderr(self):
        executor = agent.CommandExecutor(
            {
                "foo": "bar", "thread": "sometimes",
                "stdout": "to", "stderr": "hell"
            },
            {
                "resp": "foobar"
            })
        executor._get_redirection = mock.Mock()

        executor._get_stdout_stderr()

        self.assertEqual(
            [
                mock.call("to", thread="sometimes"),
                mock.call("hell", thread="sometimes", is_stderr=True)
            ],
            executor._get_redirection.mock_calls)

    def _get_executor_for_run(self, thread=False):
        req = {
            "foo": "bar",
            "path": ["some", "path", "to", "executable"],
            "thread": thread
        }

        resp = {"resp": "foobar"}
        executor = agent.CommandExecutor(req, resp)

        return executor, req, resp

    @mock.patch("subprocess.Popen")
    def test_run(self, mock_subprocess_popen):
        executor, req, resp = self._get_executor_for_run()
        executor._get_stdout_stderr = mock.Mock(
            return_value=("stdout_fh", "stderr_fh"))

        mock_subprocess_popen.return_value.communicate.return_value = (
            "stdout_out", "stderr_out")
        mock_subprocess_popen.return_value.wait.return_value = "barfoo"


        new_resp = executor.run()

        self.assertEqual(new_resp, resp)
        self.assertEqual(
            {
                "exit_code": "barfoo",
                "resp": "foobar",
                "stdout": "stdout_out",
                "stderr": "stderr_out"
            },
            resp)

        mock_subprocess_popen.assert_called_once_with(
            req["path"], stdout="stdout_fh", stderr="stderr_fh")
        self.assertEqual(
            [
                mock.call.communicate(),
                mock.call.wait()
            ],
            mock_subprocess_popen.return_value.mock_calls)

    @mock.patch("subprocess.Popen")
    @mock.patch("threading.Thread")
    @mock.patch("agent.open", side_effect=["stdout_new_fh", "stderr_new_fh"])
    def test_run_thread(self, mock_agent_open,
                        mock_threading_thread, mock_subprocess_popen):
        executor, req, resp = self._get_executor_for_run(thread=True)

        mock_stdout = mock.Mock(name="stdout")
        mock_stderr = mock.Mock(name="stderr")
        executor._get_stdout_stderr = mock.Mock(
            return_value=(mock_stdout, mock_stderr))

        mock_subprocess_popen.return_value.wait.return_value = "barfoo"

        new_resp = executor.run()

        self.assertEqual(new_resp, resp)
        self.assertEqual(
            {
                "resp": "foobar",
                "stdout_fh": mock_stdout.name,
                "stderr_fh": mock_stderr.name
            },
            resp)

        mock_subprocess_popen.assert_called_once_with(
            req["path"], stdout=mock_stdout, stderr=mock_stderr)

        mock_threading_thread.assert_called_once_with(
            target=executor._thread_target,
            args=(mock_subprocess_popen.return_value,))
        mock_threading_thread.return_value.start.assert_called_once_with()
        self.assertEqual(mock_threading_thread.return_value,
                         executor.thread)

        self.assertEqual(
            [
                mock.call(mock_stdout.name, "rb"),
                mock.call(mock_stderr.name, "rb"),
            ],
            mock_agent_open.mock_calls
        )

        self.assertEqual("stdout_new_fh", executor.stdout_fh)
        self.assertEqual("stderr_new_fh", executor.stderr_fh)

        self.assertEqual(mock_stdout, executor.child_stdout_fh)
        self.assertEqual(mock_stderr, executor.child_stderr_fh)

    def test_clear(self):
        executor = agent.CommandExecutor({}, {})

        def get_names():
            for name in ("stdout_fh", "stderr_fh"):
                for prefix in ("", "child_"):
                    yield prefix + name

        for name in get_names():
            setattr(executor, name, mock.Mock())

        executor.clear()

        for name in get_names():
            getattr(executor, name).close.assert_called_once_with()


@ddt.ddt
class AgentTestCase(unittest.TestCase):
    mocks = []

    def _start_zmq_mocks(self):
        # FIXME(pboldin): this should be done clearer
        self.mocks = [
            mock.patch("agent.Agent.init_subscribe_zmq"),
            mock.patch("agent.Agent.init_push_zmq")
        ]

        return [mock_.start() for mock_ in self.mocks]

    def _stop_zmq_mocks(self):
        for mock in self.mocks:
            mock.stop()

    def tearDown(self):
        super(AgentTestCase, self).tearDown()
        self._stop_zmq_mocks()

    @mock.patch("uuid.uuid4", return_value="uuid4")
    def test___init__(self, mock_uuid_uuid4):
        mock_init_subscribe_zmq, mock_init_push_zmq = self._start_zmq_mocks()

        agent_instance = agent.Agent("subscribe_url", "push_url")
        self.assertEqual(mock_uuid_uuid4.return_value, agent_instance.agent_id)

        mock_init_subscribe_zmq.assert_called_once_with("subscribe_url")
        mock_init_push_zmq.assert_called_once_with("push_url")

    @mock.patch("zmq.Context")
    def test_init_zmq(self, mock_zmq_context):
        agent_instance = agent.Agent("subscribe_url", "push_url")

        self.assertEqual(
            [
                # SUB socket
                # zmq.Context()
                mock.call(),
                # context.socket(zmq.SUB)
                mock.call().socket(zmq.SUB),
                # socket.connect
                mock.call().socket().connect("subscribe_url"),
                # socket.setsockopt_string(zmq.SUBSCRIBE...)
                mock.call().socket().setsockopt_string(zmq.SUBSCRIBE, u""),

                # PUSH socket
                # zmq.Context()
                mock.call(),
                # context.socket(zmq.PUSH)
                mock.call().socket(zmq.PUSH),
                # socket.connect
                mock.call().socket().connect("push_url"),
            ],
            mock_zmq_context.mock_calls)

    @ddt.data({
                "agent_id": "abc",
                "recv_json": {"target": "abc",
                              "foo": "bar"},
              },
              {
                "agent_id": "abc",
                "recv_json": {"target": ["abc", "def"],
                              "foo": "bar"},
              },
              {
                "recv_json": {"target": "abc",
                              "foo": "bar"},
                "expected": None
              },
              {
                "recv_json": {"foo": "bar"},
              },
    )
    @ddt.unpack
    def test_recv_request(self, agent_id="foobar",
                          recv_json={}, expected=True):
        mock_init_subscribe_zmq, mock_init_push_zmq = self._start_zmq_mocks()
        subscribe_socket = mock_init_subscribe_zmq.return_value

        subscribe_socket.recv_json.return_value = recv_json

        agent_instance = agent.Agent("subscribe_url", "push_url",
                                     agent_id=agent_id)

        retval = agent_instance.recv_request()

        subscribe_socket.recv_json.assert_called_once_with()

        if expected is True:
            expected = recv_json
        self.assertEqual(expected, retval)

    def test_do_default(self):
        self._start_zmq_mocks()
        agent_instance = agent.Agent("subscribe_url", "push_url")
        self.assertRaises(ValueError, agent_instance.do_default,
                          {"action": "foobar"}, None)

    def test_loop_none(self):
        self._start_zmq_mocks()
        agent_instance = agent.Agent("subscribe_url", "push_url")
        agent_instance.recv_request = mock.Mock(return_value=None)

        retval = agent_instance.loop()

        agent_instance.recv_request.assert_called_once_with()

    def test_loop_unknown_action(self):
        _, mock_init_push_zmq = self._start_zmq_mocks()
        push_socket = mock_init_push_zmq.return_value

        agent_instance = agent.Agent("subscribe_url", "push_url")
        agent_instance.recv_request = mock.Mock(
            return_value={
                "action": "unknown",
                "req": "foobar"
            })

        retval = agent_instance.loop()

        push_socket.send_json.assert_called_once_with(
            {
                "error": "Action 'unknown' unknown.",
                "req": "foobar", "agent": agent_instance.agent_id
            }
        )

    def test_loop_mock_action(self):
        _, mock_init_push_zmq = self._start_zmq_mocks()
        push_socket = mock_init_push_zmq.return_value

        agent_instance = agent.Agent("subscribe_url", "push_url")
        agent_instance.recv_request = mock.Mock(
            return_value={
                "action": "mock",
                "req": "foobar"
            })
        agent_instance.do_mock = mock.Mock(return_value={"custom": "return"})

        retval = agent_instance.loop()

        push_socket.send_json.assert_called_once_with(
            {"custom": "return"}
        )

    def test_loop_mock_action_return_none(self):
        _, mock_init_push_zmq = self._start_zmq_mocks()
        push_socket = mock_init_push_zmq.return_value

        agent_instance = agent.Agent("subscribe_url", "push_url")
        agent_instance.recv_request = mock.Mock(
            return_value={
                "action": "mock",
                "req": "foobar"
            })

        def do_mock(req, resp):
            resp["foo"] = "bar"
        agent_instance.do_mock = do_mock

        retval = agent_instance.loop()

        push_socket.send_json.assert_called_once_with(
            {
                "req": "foobar",
                "agent": agent_instance.agent_id,
                "foo": "bar"
            }
        )

    @mock.patch("datetime.datetime")
    def test_do_ping(self, mock_datetime_datetime):
        self._start_zmq_mocks()
        agent_instance = agent.Agent("subscribe_url", "push_url")

        mock_datetime_datetime.utcnow.return_value.isoformat.return_value = (
            "foobar"
        )

        req = {}
        resp = {}
        agent_instance.do_ping(req, resp)

        self.assertEqual("foobar", resp["time"])
        self.assertEqual({}, req)

    def test_do_tail_no_executor(self):
        self._start_zmq_mocks()
        agent_instance = agent.Agent("subscribe_url", "push_url")

        self.assertRaises(ValueError, agent_instance.do_tail, {}, {})

    def test_do_tail(self):
        self._start_zmq_mocks()
        agent_instance = agent.Agent("subscribe_url", "push_url")

        executor = agent_instance.executor = mock.Mock()

        def mock_pipe_return(pipe, return_value):
            pipe.read.return_value.decode.return_value = return_value

        mock_pipe_return(executor.stdout_fh, "stdout")
        mock_pipe_return(executor.stderr_fh, "stderr")


        req = {"size": "4200"}
        resp = {}
        agent_instance.do_tail(req, resp)

        def assert_pipe(pipe):
            pipe.read.assert_called_once_with(4200)
            pipe.read.return_value.decode.assert_called_once_with("utf-8")

        assert_pipe(executor.stdout_fh)
        assert_pipe(executor.stderr_fh)

        self.assertEqual(
            {
                "stdout": "stdout",
                "stderr": "stderr"
            },
            resp)

    def test_do_check_no_executor(self):
        self._start_zmq_mocks()
        agent_instance = agent.Agent("subscribe_url", "push_url")

        self.assertRaises(ValueError, agent_instance.do_check, {}, {})


    @ddt.unpack
    @ddt.data(
        {"req": {"clear": "true"}},
        {"req": {"wait": "true"}},
        {"req": {"clear": "true", "wait": "true"}},
        {"req": {}},
    )
    def test_do_check(self, req):
        self._start_zmq_mocks()
        agent_instance = agent.Agent("subscribe_url", "push_url")

        executor = agent_instance.executor = mock.Mock(exit_code="foobar")

        resp = {}
        agent_instance.do_check(req, resp)

        if req.get("wait") or req.get("clear"):
            executor.thread.join.assert_called_once_with()
        self.assertEqual({"exit_code": "foobar"}, resp)
        if req.get("clear"):
            executor.clear.assert_called_once_with()
            self.assertIsNone(agent_instance.executor)

    def test_do_command_already(self):
        self._start_zmq_mocks()
        agent_instance = agent.Agent("subscribe_url", "push_url")

        agent_instance.executor = mock.Mock(thread=True)
        self.assertRaises(ValueError, agent_instance.do_command, {}, {})

    @mock.patch("agent.CommandExecutor")
    def test_do_command(self, mock_agent_command_executor):
        self._start_zmq_mocks()
        agent_instance = agent.Agent("subscribe_url", "push_url")

        req = {}
        resp = {}
        agent_instance.do_command(req, resp)

        mock_agent_command_executor.assert_called_once_with(req, resp)
        self.assertEqual(mock_agent_command_executor.return_value,
                         agent_instance.executor)
        mock_agent_command_executor.return_value.run.assert_called_once_with()
