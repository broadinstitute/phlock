from flock import Task
from flock.queue.lsf import LSFQueue
import mock
import subprocess

JOB_OUTPUT = ("JOBID   USER    STAT  QUEUE      FROM_HOST   EXEC_HOST   JOB_NAME   SUBMIT_TIME\n"+
              "6265422 pmontgo PEND  bhour      tin                     *h -c echo May  9 17:11\n")

def mock_popen(stdout="", stderr=""):
    handle = mock.Mock()
    handle.communicate = mock.Mock(return_value=(stdout, stderr))
    m = mock.Mock(return_value=handle)
    return m


popen_qstat_mock = mock_popen(JOB_OUTPUT)
@mock.patch("flock.queue.lsf.subprocess.Popen", popen_qstat_mock)
def test_get_jobs():
    listener = mock.Mock()
    queue = LSFQueue(listener, "", "", "", "workdir")

    jobs = queue.get_jobs_from_external_queue()
    popen_qstat_mock.assert_called_once_with(['bjobs', "-w"], stdout=subprocess.PIPE)
    assert len(jobs) == 1


qsub_popen_mock = mock_popen("Job <6265891> is submitted to queue <bhour>.\n")


@mock.patch("flock.queue.lsf.subprocess.Popen", qsub_popen_mock)
def test_add_to_queue():
    listener = mock.Mock()
    queue = LSFQueue(listener, "", "", "", "workdir")
    queue.add_to_queue("/home/task", "normal", "/home/task/task.sh", "/home/task/stdout.txt", "/home/task/stderr.txt")

    qsub_popen_mock.assert_called_once_with(
        ['bsub', '-o', '/home/task/stdout.txt', '-e', '/home/task/stderr.txt', '-cwd', 'workdir', 'bash /home/task/task.sh'], stdout=subprocess.PIPE)


qdel_popen_mock = mock_popen("Killed")


@mock.patch("flock.queue.lsf.subprocess.Popen", qdel_popen_mock)
def test_kill():
    listener = mock.Mock()
    queue = LSFQueue(listener, "", "", "", "workdir")
    queue.kill([Task("task", "100", "running", "/home/task")])

    qdel_popen_mock.assert_called_once_with(["bkill", "100"])
