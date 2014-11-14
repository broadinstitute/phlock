import subprocess
from __init__ import AbstractQueue
from util import split_options, divide_into_batches
import re
import xml.etree.ElementTree as ETree
import flock
import logging

log = logging.getLogger("flock")

def rewrite_options_with_override(options, override_req_mem_in_megs):
    options = list(options)
    if override_req_mem_in_megs != None:
        # this isn't actually correct parsing of args, but is a hack relying
        # on it should be rare that we have a param "-l"
        # drop all of the current constraints
        i = 0
        while i < len(options):
            if options[i] == '-l':
                # if we have a constraint specified via "-l", drop this and the next parameter
                del options[i]
                del options[i]

        # now we should have a clean list of arguments with no "-l h_vmem=16G,virtual_free=16G"
        # so add on our own
        options.append("-l")
        options.append("h_vmem=%dM,virtual_free=%dM" % (override_req_mem_in_megs, override_req_mem_in_megs))
    return options

class SGEQueue(AbstractQueue):
    def __init__(self, listener, qsub_options, scatter_qsub_options, name, workdir, override_req_mem_in_megs=None):
        super(SGEQueue, self).__init__(listener)
        self.qsub_options = rewrite_options_with_override(split_options(qsub_options), override_req_mem_in_megs)
        self.scatter_qsub_options = rewrite_options_with_override(split_options(scatter_qsub_options), override_req_mem_in_megs)
        self.external_id_prefix = "SGE:"

        self.name = name
        self.safe_name = re.sub("\\W+", "-", name)
        self.workdir = workdir

    def get_jobs_from_external_queue(self):
        handle = subprocess.Popen(["qstat", "-xml"], stdout=subprocess.PIPE)
        stdout, stderr = handle.communicate()

        doc = ETree.fromstring(stdout)
        job_list = doc.findall(".//job_list")

        active_jobs = {}
        for job in job_list:
            job_id = job.find("JB_job_number").text

            state = job.attrib['state']
            if state == "running":
                active_jobs[job_id] = flock.RUNNING
            elif state == "pending":
                active_jobs[job_id] = flock.SUBMITTED
            else:
                active_jobs[job_id] = flock.QUEUED_UNKNOWN
        return active_jobs

    def add_to_queue(self, task_full_path, is_scatter, script_to_execute, stdout_path, stderr_path):
        d = task_full_path

        task_path_comps = d.split("/")
        task_name = task_path_comps[-1]
        if not task_name[0].isalpha():
            task_name = "t" + task_name

        job_name = "%s-%s" % (task_name, self.safe_name)

        cmd = ["qsub", "-N", job_name, "-V", "-b", "n", "-cwd", "-o", stdout_path, "-e", stderr_path]
        if is_scatter:
            cmd.extend(self.scatter_qsub_options)
        else:
            cmd.extend(self.qsub_options)
        cmd.extend([script_to_execute])
        log.info("EXEC: %s", cmd)
        handle = subprocess.Popen(cmd, stdout=subprocess.PIPE, cwd=self.workdir)
        stdout, stderr = handle.communicate()

        # Stdout Example:
        #Your job 3 ("task.sh") has been submitted

        bjob_id_pattern = re.compile("Your job (\\d+) \\(.* has been submitted.*")
        m = bjob_id_pattern.match(stdout)
        if m == None:
            raise Exception("Could not parse output from qsub: %s" % stdout)

        sge_job_id = m.group(1)
        self.listener.task_submitted(d, self.external_id_prefix + sge_job_id)

    def kill(self, tasks):
        for batch in divide_into_batches(tasks, 100):
            cmd = ["qdel"]
            cmd.extend([task.external_id for task in batch])
            handle = subprocess.Popen(cmd)
            handle.communicate()
