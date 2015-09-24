#!/usr/bin/python

import ddt
import mock
import subprocess
import unittest

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


class AgentTestCase(unittest.TestCase):
    pass
