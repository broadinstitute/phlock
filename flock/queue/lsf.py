from __init__ import AbstractQueue
from util import *
import logging
import subprocess
import re
import flock

log = logging.getLogger("flock")


class LSFQueue(AbstractQueue):
    def __init__(self, listener, bsub_options, scatter_bsub_options, workdir):
        super(LSFQueue, self).__init__(listener)
        self.bsub_options = split_options(bsub_options)
        self.scatter_bsub_options = split_options(scatter_bsub_options)
        self.workdir = workdir
        self.external_id_prefix = "LSF:"

    def get_jobs_from_external_queue(self):
        handle = subprocess.Popen(["bjobs", "-w"], stdout=subprocess.PIPE)
        stdout, stderr = handle.communicate()

        # Output looks like:
        #  JOBID   USER    STAT  QUEUE      FROM_HOST   EXEC_HOST   JOB_NAME   SUBMIT_TIME
        #  6265422 pmontgo PEND  bhour      tin                     *h -c echo May  9 17:11
        # or
        #  No unfinished job found
        lines = stdout.split("\n")
        job_pattern = re.compile("\\s*(\\d+)\\s+\\S+\\s+(\\S+)\\s+.*")
        active_jobs = {}
        for line in lines[1:]:
            if line == '':
                continue
            m = job_pattern.match(line)
            if m == None:
                log.warning("Could not parse line from bjobs: %s", repr(line))
            else:
                job_id = m.group(1)
                job_state = m.group(2)
                if job_state == "PEND":
                    s = flock.SUBMITTED
                elif job_state == "RUN":
                    s = flock.RUNNING
                else:
                    s = flock.QUEUED_UNKNOWN
                active_jobs[job_id] = s
        return active_jobs

    def add_to_queue(self, task_full_path, is_scatter, script_to_execute, stdout, stderr):
        d = task_full_path
        cmd = ["bsub", "-o", stdout, "-e", stderr, "-cwd", self.workdir]
        if task_type == "scatter":
            cmd.extend(self.scatter_bsub_options)
        elif task_type == "gather":
            cmd.extend(self.gather_bsub_options)
        elif task_type == "normal":
            cmd.extend(self.bsub_options)
        else:
            raise Exception("Invalid task type: %r" % (task_type,))
        cmd.append("bash %s" % script_to_execute)
        log.info("EXEC: %s", cmd)
        handle = subprocess.Popen(cmd, stdout=subprocess.PIPE)
        stdout, stderr = handle.communicate()

        # Stdout Example:
        #Job <6265891> is submitted to queue <bhour>.

        bjob_id_pattern = re.compile("Job <(\\d+)> is submitted.*")
        m = bjob_id_pattern.match(stdout)
        if m == None:
            raise Exception("Could not parse output from bsub: %s" % stdout)

        lsf_job_id = m.group(1)
        self.listener.task_submitted(d, self.external_id_prefix + lsf_job_id)

    def kill(self, tasks):
        for batch in divide_into_batches(tasks, 100):
            cmd = ["bkill"]
            cmd.extend([task.external_id for task in batch])
            handle = subprocess.Popen(cmd)
            handle.communicate()


