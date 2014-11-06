from __init__ import *
import getpass
import subprocess
import re
import logging
import signal

log = logging.getLogger("flock")

class LocalBgQueue(AbstractQueue):
    def __init__(self, listener, workdir):
        super(LocalBgQueue, self).__init__(listener)
        self.workdir = workdir
        self.external_id_prefix = "PID:"

    def get_jobs_from_external_queue(self):

        cmd = ["ps", "-o", "pid", "-u", getpass.getuser()]
        log.info("executing: %s", cmd)
        handle = subprocess.Popen(cmd, stdout=subprocess.PIPE)
        stdout, stderr = handle.communicate()

        # Output looks like:
        # PID
        # 2587
        # 8812
        lines = stdout.split("\n")
        job_pattern = re.compile("\\s*(\\d+)\\s*")
        active_jobs = {}
        for line in lines[1:]:
            if line == '':
                continue
            m = job_pattern.match(line)
            if m == None:
                log.warning("Could not parse line from ps: %s", repr(line))
            else:
                pid = m.group(1)
                active_jobs[pid] = flock.RUNNING
        return active_jobs

    def add_to_queue(self, task_full_path, is_scatter, script_to_execute, stdout_path, stderr_path):
        d = task_full_path
        stdout = open(stdout_path, "a")
        stderr = open(stderr_path, "a")
        cmd = ["bash", script_to_execute]
        log.info("executing: %s", cmd)
        handle = subprocess.Popen(cmd, stdout=stdout, stderr=stderr, cwd=self.workdir)
        stdout.close()
        stderr.close()

        self.listener.task_submitted(d, self.external_id_prefix + str(handle.pid))

    def kill(self, tasks):
        for task in tasks:
            os.kill(int(task.external_id), signal.SIGINT)


class LocalQueue(AbstractQueue):
    def __init__(self, listener, workdir):
        self._ran = set()
        self._extern_ids = {}
        super(LocalQueue, self).__init__(listener)
        self.workdir = workdir
        self.external_id_prefix = "INVALID:"

    def get_jobs_from_external_queue(self):
        return {}

    def add_to_queue(self, task_full_path, is_scatter, script_to_execute, stdout, stderr):
        d = task_full_path
        cmd = "cd %s ; bash %s >> %s 2>> %s" % (self.workdir, script_to_execute, stdout, stderr)
        if cmd in self._ran:
            raise Exception("Already ran %s once" % cmd)
        self.system(cmd, ignore_retcode=True)
        self._extern_ids[task_full_path] = str(len(self._extern_ids))
        self._ran.add(cmd)

    def kill(self, task):
        raise Exception("not implemented")
