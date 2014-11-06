from flock import Task
from flock.queue.sge import SGEQueue
import mock
import subprocess

JOB_XML = """<?xml version='1.0'?>
<job_info  xmlns:xsd="http://gridscheduler.svn.sourceforge.net/viewvc/gridscheduler/trunk/source/dist/util/resources/schemas/qstat/qstat.xsd?revision=11">
  <queue_info>
    <job_list state="running">
      <JB_job_number>561860</JB_job_number>
      <JAT_prio>0.55256</JAT_prio>
      <JB_name>t081-20141031-095702-0</JB_name>
      <JB_owner>ubuntu</JB_owner>
      <state>r</state>
      <JAT_start_time>2014-10-31T16:14:39</JAT_start_time>
      <queue_name>all.q@node001</queue_name>
      <slots>1</slots>
    </job_list>
  </queue_info>
</job_info>"""


def mock_popen(stdout="", stderr=""):
    handle = mock.Mock()
    handle.communicate = mock.Mock(return_value=(stdout, stderr))
    m = mock.Mock(return_value=handle)
    return m


popen_qstat_mock = mock_popen(JOB_XML)


@mock.patch("subprocess.Popen", popen_qstat_mock)
def test_get_jobs():
    listener = mock.Mock()
    queue = SGEQueue(listener, "", "", "name", "workdir")

    jobs = queue.get_jobs_from_external_queue()
    popen_qstat_mock.assert_called_once_with(['qstat', '-xml'], stdout=subprocess.PIPE)
    assert len(jobs) == 1


qsub_popen_mock = mock_popen("Your job 3 (\"name\") has been submitted")


@mock.patch("subprocess.Popen", qsub_popen_mock)
def test_add_to_queue():
    listener = mock.Mock()
    queue = SGEQueue(listener, "", "", "name", "workdir")
    queue.add_to_queue("/home/task", False, "/home/task/task.sh", "/home/task/stdout.txt", "/home/task/stderr.txt")

    qsub_popen_mock.assert_called_once_with(
        ["qsub", "-N", "task-name", "-V", "-b", "n", "-cwd", "-o", "/home/task/stdout.txt", "-e",
         "/home/task/stderr.txt", "/home/task/task.sh"], stdout=subprocess.PIPE, cwd="workdir")


qdel_popen_mock = mock_popen("Killed")


@mock.patch("subprocess.Popen", qdel_popen_mock)
def test_kill():
    listener = mock.Mock()
    queue = SGEQueue(listener, "", "", "name", "workdir")
    queue.kill([Task("task", "100", "running", "/home/task")])

    qdel_popen_mock.assert_called_once_with(["qdel", "100"])
