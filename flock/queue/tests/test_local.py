from flock import Task
from flock.queue.local import LocalBgQueue
import mock
import subprocess
import signal


def mock_popen(stdout="", stderr=""):
    handle = mock.Mock()
    handle.communicate = mock.Mock(return_value=(stdout, stderr))
    m = mock.Mock(return_value=handle)
    return m


jobs_popen_mock = mock_popen("PID\n2587\n")
@mock.patch("subprocess.Popen", jobs_popen_mock)
@mock.patch("flock.queue.local.getpass.getuser", mock.Mock(return_value="username"))
def test_get_jobs():
    listener = mock.Mock()
    queue = LocalBgQueue(listener, "workdir")

    jobs = queue.get_jobs_from_external_queue()
    jobs_popen_mock.assert_called_once_with(['ps', '-o', 'pid', '-u', 'username'], stdout=subprocess.PIPE)
    assert len(jobs) == 1


# sub_popen_mock = mock_popen("")
# @mock.patch("subprocess.Popen", sub_popen_mock)
# def test_add_to_queue():
#     listener = mock.Mock()
#     queue = LocalBgQueue(listener, "workdir")
#     queue.add_to_queue("/home/task", False, "/home/task/task.sh", "/home/task/stdout.txt", "/home/task/stderr.txt")
#
#     sub_popen_mock.assert_called_once_with(
#         ["qsub", "-N", "task-name", "-V", "-b", "n", "-cwd", "-o", "/home/task/stdout.txt", "-e",
#          "/home/task/stderr.txt", "/home/task/task.sh"], stdout=subprocess.PIPE, cwd="workdir")


kill_mock = mock.Mock(name="kill")

@mock.patch("os.kill", kill_mock)
def test_kill():
    listener = mock.Mock()
    queue = LocalBgQueue(listener, "workdir")
    queue.kill([Task("task", "100", "running", "/home/task")])

    kill_mock.assert_called_once_with(100, signal.SIGINT)
