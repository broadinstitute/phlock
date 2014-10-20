import argparse
import sys
import collections
import os
import glob
import subprocess
import re
import time
import logging
import socket
import json
import base64
import hashlib
import xml.etree.ElementTree as ETree

FLOCK_VERSION = "1.0"

# the various status codes for tasks
CREATED = "Created"
SUBMITTED = "Submitted"
RUNNING = "Running"
FINISHED = "Finished"
QUEUED_UNKNOWN = "Queued, with unknown status"
FAILED = "Failed"
WAITING = "Waiting for other tasks to finish"
UNKNOWN = "Missing"

Task = collections.namedtuple("Task", ["task_dir", "external_id", "status", "full_path"])

log = logging.getLogger("flock")

def timeit(method):
    def timed(*args, **kw):
        log.debug("starting %r", method.__name__)
        start = time.time()
        result = method(*args, **kw)
        end = time.time()

        log.debug("executed %r in %.2f secs", method.__name__, (end - start))
        return result

    return timed


def read_task_dirs(run_id):
    """ returns a tuple of (task_dirs, job_deps) where
        task_dirs is a list of task directories
        job_deps is a map of task_dir -> set of tasks that must complete before this can start
    """
    task_dirs = []
    job_deps = collections.defaultdict(lambda: set())
    for dirname in glob.glob("%s/tasks*" % run_id):
        fn = "%s/task_dirs.txt" % dirname
        if os.path.exists(fn):
            grouped_commands = {}
            with open(fn) as fd:
                for line in fd.readlines():
                    line = line.strip()
                    i = line.find(" ")
                    group = int(line[:i])
                    command = line[i + 1:]

                    if group in grouped_commands:
                        commands = grouped_commands[group]
                    else:
                        commands = []
                        grouped_commands[group] = commands

                    commands.append(command)
            # now, we'll assume we have two groups: 1 and 2
            # where 1 is all the scatter jobs, and 2 is the gather.
            # (In practice that's what happens)
            scatter_tasks = grouped_commands[1]
            gather_tasks = []
            if 2 in grouped_commands:
                gather_tasks = grouped_commands[2]
            task_dirs.extend(scatter_tasks)
            task_dirs.extend(gather_tasks)
            for gather_task in gather_tasks:
                job_deps[gather_task] = set(scatter_tasks)
    return (task_dirs, job_deps)


def finished_successfully(run_id, task_dir):
    finished = os.path.exists("%s/%s/finished-time.txt" % (run_id, task_dir))
    # print "is_finished %s/finished-time.txt -> %s" % (task_dir, finished)
    return finished


class TaskStatusCache:
    def __init__(self):
        self.missing_since = collections.defaultdict(lambda: None)
        self.finished_successfully = set()
        # history is tuples of (timstamp, finished_count) ordered by timestamp
        self.history = []

    def update_estimate(self, tasks):
        counts = collections.defaultdict(lambda: 0)
        for task in tasks:
            counts[task.status] += 1
        self.record_finished_count(time.time(), counts[FINISHED] + counts[FAILED] + counts[UNKNOWN])
        return self.estimate_completion_rate(
            counts[SUBMITTED] + counts[WAITING] + counts[RUNNING] + counts[QUEUED_UNKNOWN])

    def record_finished_count(self, timestamp, finished_count):
        self.history.append((timestamp, finished_count))

    def estimate_completion_rate(self, submitted_count, window=60 * 10):
        last_allowed = time.time() - window

        if len(self.history) < 2:
            return None

        # get rid of entries which are too old, or where the finished count is actually greater then our current finished count.  (implying jobs went from finished->running, such as when retrying)
        last = self.history[-1]
        history = [(timestamp, finished_count) for timestamp, finished_count in self.history if
                   timestamp > last_allowed and finished_count <= last]

        if len(history) < 2:
            return None

        last = history[-1]
        first = history[0]

        completions_per_minute = (last[1] - first[1]) / (last[0] - first[0]) * 60
        if completions_per_minute == 0.0:
            return None

        return (submitted_count / completions_per_minute, completions_per_minute)

    def update_failure(self, task_dir, is_running):
        if is_running:
            self.missing_since[task_dir] = None
        else:
            if self.missing_since[task_dir] == None:
                self.missing_since[task_dir] = time.time()

    def definitely_failed(self, task_dir):
        " returns true if we're sure we've failed -- that is, we've been missing this task for > 5 seconds "
        t = self.missing_since[task_dir]
        if t == None:
            return False
        return time.time() - t > 5

    def _finished_successfully(self, run_id, task_dir):
        if not (task_dir in self.finished_successfully) and finished_successfully(run_id, task_dir):
            self.finished_successfully.add(task_dir)
        return task_dir in self.finished_successfully

    def get_status(self, run_id, external_ids, queued_job_states, task_dir, job_deps):
        assert type(queued_job_states) == dict

        if self._finished_successfully(run_id, task_dir):
            return FINISHED

        if task_dir in external_ids:
            lsf_id = external_ids[task_dir]
            if lsf_id in queued_job_states:
                self.update_failure(task_dir, True)
                return queued_job_states[lsf_id]
            else:
                self.update_failure(task_dir, False)
                if self.definitely_failed(task_dir):
                    return FAILED
                else:
                    return UNKNOWN
        else:
            all_deps_met = True
            for dep in job_deps[task_dir]:
                if self.get_status(run_id, external_ids, queued_job_states, dep, job_deps) != FINISHED:
                    all_deps_met = False
            if all_deps_met:
                return CREATED
            else:
                return WAITING


@timeit
def find_tasks(run_id, external_ids, queued_job_states, task_dirs, job_deps, cache):
    def get_status(task_dir):
        return cache.get_status(run_id, external_ids, queued_job_states, task_dir, job_deps)

    def get_external_id(task_dir):
        return external_ids[task_dir] if task_dir in external_ids else None

    # for task_dirs in grouped_dirs:
    tasks = []
    tasks.extend(
        [Task(task_dir, get_external_id(task_dir), get_status(task_dir), run_id + "/" + task_dir) for task_dir in
         task_dirs])

    return tasks


@timeit
def read_external_ids(run_id, task_dirs, expected_prefix):
    external_ids = collections.defaultdict(lambda: [])
    for task_dir in task_dirs:
        job_id_file = "%s/%s/job_id.txt" % (run_id, task_dir)
        if os.path.exists(job_id_file):
            with open(job_id_file) as fd:
                job_id = fd.read()
                assert job_id.startswith(expected_prefix), "Job ID was expected to be %s but was %s" % (
                expected_prefix, job_id)
                external_ids[task_dir] = job_id[len(expected_prefix):]
    return external_ids

class AbstractQueue(object):
    def __init__(self, listener):
        self.cache = TaskStatusCache()
        self.last_estimate = None
        self.listener = listener

    def submit(self, run_id, task_full_path, is_scatter):
        self.clean_task_dir(task_full_path)
        d = task_full_path

        stdout = "%s/stdout.txt" % d
        stderr = "%s/stderr.txt" % d
        script_to_execute = "%s/task.sh" % d

        script_to_execute, stdout, stderr = self.listener.presubmit(run_id, task_full_path, script_to_execute, stdout, stderr)

        self.add_to_queue(task_full_path, is_scatter, script_to_execute, stdout, stderr)

    def get_last_estimate(self):
        return self.last_estimate

    def find_tasks(self, run_id):
        task_dirs, job_deps = read_task_dirs(run_id)
        queued_job_states = self.get_jobs_from_external_queue()
        external_ids = read_external_ids(run_id, task_dirs, self.external_id_prefix)
        tasks = find_tasks(run_id, external_ids, queued_job_states, task_dirs, job_deps, self.cache)
        self.last_estimate = self.cache.update_estimate(tasks)
        return tasks

    def clean_task_dir(self, task_full_path):
        for fn in ["%s/stdout.txt" % task_full_path, "%s/stderr.txt" % task_full_path]:
            if os.path.exists(fn):
                for i in xrange(20):
                    dest = "%s.%d" % (fn, i)
                    if not os.path.exists(dest):
                        break
                os.rename(fn, dest)


class JobListener(object):
    def task_submitted(self, task_dir, external_id):
        with open("%s/job_id.txt" % task_dir, "w") as fd:
            fd.write(external_id)

    def presubmit(self, run_id, task_full_path, task_script, stdout, stderr):
        return (task_script, stdout, stderr)

import xmlrpclib

class ConsolidatedMonitor(JobListener):
    def __init__(self, endpoint_url, flock_home):
        self.endpoint_url = endpoint_url
        self.flock_home = flock_home
        self.service = xmlrpclib.ServerProxy(endpoint_url)

        # just make sure we can connect and its working
        version = self.service.get_version()
        assert version != None

    def task_submitted(self, task_dir, external_id):
        JobListener.task_submitted(self, task_dir, external_id)
        self.service.task_submitted(task_dir, external_id)

    def presubmit(self, run_id, task_full_path, task_script, stdout, stderr):
        d = task_full_path
        script_to_execute = "%s/wrapped_task.sh" % d
        with open(script_to_execute, "w") as fd:
            fd.write("set -e\n"
                     "python %(flock_home)s/notify.py %(endpoint_url)s started %(run_id)s %(task_dir)s\n"
                     "set +e\n"
                     "if bash %(task_script)s ; then\n"
                     "  set -e\n"
                     "  python %(flock_home)s/notify.py %(endpoint_url)s completed %(run_id)s %(task_dir)s\n"
                     "else\n"
                     "  set -e\n"
                     "  python %(flock_home)s/notify.py %(endpoint_url)s failed %(run_id)s %(task_dir)s\n"
                     "fi\n" % dict(endpoint_url=self.endpoint_url,
                                   run_id=run_id,
                                   task_dir=d,
                                   task_script=task_script,
                                   flock_home=self.flock_home))

        return (script_to_execute, stdout, stderr)


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


def split_options(s):
    s = s.strip()
    if len(s) == 0:
        return []
    else:
        return s.split(" ")


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
                    s = SUBMITTED
                elif job_state == "RUN":
                    s = RUNNING
                else:
                    s = QUEUED_UNKNOWN
                active_jobs[job_id] = s
        return active_jobs

    def add_to_queue(self, task_full_path, is_scatter, script_to_execute, stdout, stderr):
        d = task_full_path
        cmd = ["bsub", "-o", stdout, "-e", stderr, "-cwd", self.workdir]
        if is_scatter:
            cmd.extend(self.bsub_options)
        else:
            cmd.extend(self.scatter_bsub_options)
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

    def kill(self, task):
        raise Exception("bkill %s" % task.external_id)


class SGEQueue(AbstractQueue):
    def __init__(self, listener, qsub_options, scatter_qsub_options, name, workdir):
        super(SGEQueue, self).__init__(listener)
        self.qsub_options = split_options(qsub_options)
        self.scatter_qsub_options = split_options(scatter_qsub_options)
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
                active_jobs[job_id] = RUNNING
            elif state == "pending":
                active_jobs[job_id] = SUBMITTED
            else:
                active_jobs[job_id] = QUEUED_UNKNOWN
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


def divide_into_batches(elements, size):
    for i in range(0, len(elements), size):
        yield elements[i:i + size]


class LocalBgQueue(AbstractQueue):
    def __init__(self, listener, workdir):
        super(LocalBgQueue, self).__init__(listener)
        self.workdir = workdir
        self.external_id_prefix = "PID:"

    def get_jobs_from_external_queue(self):
        import getpass

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
                active_jobs[pid] = RUNNING
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

    def kill(self, task):
        raise Exception("bkill %s" % task.external_id)


def dump_file(filename):
    if os.path.exists(filename):
        with open(filename) as fd:
            for line in fd.readlines():
                sys.stdout.write("  ")
                sys.stdout.write(line)
    else:
        sys.stdout.write("  [ File %s does not exist ]" % filename)


class Flock(object):
    def __init__(self, job_queue, flock_home, notify_command):
        self.job_queue = job_queue
        self.flock_home = flock_home
        self.notify_command = notify_command

    def system(self, cmd, ignore_retcode=False):
        log.info("EXEC %s", repr(cmd))
        retcode = subprocess.call(cmd, shell=True)
        if retcode != 0 and (not ignore_retcode):
            raise Exception("Command terminated with exit status = %d" % retcode)

    def wait_for_completion(self, run_id, maxsubmit):
        is_complete = False
        sleep_time = 1
        while not is_complete:
            time.sleep(sleep_time)
            sleep_time = min(30, sleep_time * 2)
            is_complete, submitted_count = self.poll_once(run_id, maxsubmit)
            if submitted_count > 0:
                sleep_time = 1

        # tasks are all done, but were they all successful?
        tasks = self.job_queue.find_tasks(run_id)
        failures = []
        for task in tasks:
            if task.status == FAILED:
                failures.append(task)
        if len(failures) == 0:
            log.info("Run %s completed successfully" % run_id)
        else:
            for task in failures:
                log.warn("The following task failed: %s", task.full_path)

            failure_dir = failures[0].full_path
            log.info("Dumping stdout of first failure")
            dump_file("%s/stdout.txt" % failure_dir)
            log.info("Dumping stderr of first failure")
            dump_file("%s/stderr.txt" % failure_dir)
            log.warn("Run failed (%d tasks failed). Exitting", len(failures))
            sys.exit(1)

    def run(self, run_id, script_body, wait, maxsubmit, test_job_count, no_poll=False):
        run_dir = os.path.abspath(run_id)
        if os.path.exists(run_id):
            log.error("\"%s\" already exists. Aborting.", run_id)
            sys.exit(1)

        os.makedirs("%s/temp" % run_id)
        os.makedirs("%s/tasks-init/scatter" % run_id)
        os.makedirs("%s/tasks" % run_id)
        temp_run_script = "%s/tasks-init/scatter/scatter.R" % run_id
        with open(temp_run_script, "w") as fd:

            fd.write("flock_starting_file <- '%s/tasks-init/scatter/started-time.txt'\n" % run_dir)
            fd.write("flock_completion_file <- '%s/tasks-init/scatter/finished-time.txt'\n" % run_dir)
            fd.write("flock_test_job_count <- %s\n" % ("NA" if test_job_count == None else test_job_count))
            fd.write("flock_version <- c(%s);\n" % ", ".join(FLOCK_VERSION.split(".")))
            fd.write("flock_run_dir <- '%s';\n" % (run_dir))
            fd.write("flock_home <- '%s';\n" % (self.flock_home))
            if self.notify_command:
                fd.write("flock_notify_command <- '%s';\n" % self.notify_command)
            else:
                fd.write("flock_notify_command <- NULL;\n")

            fd.write("""fileConn<-file(flock_starting_file)
      writeLines(format(Sys.time(), "%a %b %d %X %Y"), fileConn)
      close(fileConn)
      """)

            fd.write("source('%s/flock_support.R');\n" % self.flock_home)
            fd.write(script_body)
            fd.write("""# write out record that task completed successfully
      fileConn<-file(flock_completion_file)
      writeLines(format(Sys.time(), "%a %b %d %X %Y"), fileConn)
      close(fileConn)
      """)

        with open("%s/tasks-init/scatter/task.sh" % run_id, "w") as fd:
            fd.write("R --vanilla < %s" % temp_run_script)

        with open("%s/tasks-init/task_dirs.txt" % run_id, "w") as fd:
            fd.write("1 tasks-init/scatter\n")

        if not no_poll:
            self.poll_once(run_id, maxsubmit)
            if wait:
                self.wait_for_completion(run_id, maxsubmit)
            else:
                print "Jobs are running, but --nowait was specified, so exiting"

    def print_task_table(self, rows, estimate, summarize=True):
        if summarize:
            jobs_per_status = collections.defaultdict(lambda: [])
            for task_dir, external_id, status in rows[1:]:
                jobs_per_status[status].append(task_dir)
            ks = jobs_per_status.keys()
            ks.sort()
            rows = [['Status', 'Tasks', 'Task Ids']]
            for k in ks:
                examples = jobs_per_status[k]
                count = len(examples)
                if count > 4:
                    examples = examples[:4] + ["..."]
                rows.append((k, count, " ".join(examples)))

        col_count = len(rows[0])
        col_widths = [max([len(str(row[i])) for row in rows]) + 3 for i in xrange(col_count)]

        for row in rows:
            row_str = []
            for i in xrange(col_count):
                cell = str(row[i])
                row_str.append(cell)
                row_str.append(" " * (col_widths[i] - len(cell)))
            print "  " + ("".join(row_str))

        if estimate != None:
            minutes_remaining, completions_per_minute = estimate
            print "  %.1f jobs are completing per minute. Estimated completion in %.1f minutes" % (
            completions_per_minute, minutes_remaining)
        sys.stdout.flush()

    def check_and_print(self, run_id):
        tasks = self.job_queue.find_tasks(run_id)
        estimate = self.job_queue.get_last_estimate()
        rows = [["Task", "ID", "Status"]]
        for task in tasks:
            rows.append((task.task_dir, task.external_id, task.status))
        log.info("Checking on %s" % run_id)
        self.print_task_table(rows, estimate)
        return tasks

    def list_failures(self, run_id):
        tasks = self.job_queue.find_tasks(run_id)
        for task in tasks:
            if task.status in [FAILED, UNKNOWN]:
                print task.full_path

    def write_json_summary(self, tasks, run_id):
        jobs_per_status = collections.defaultdict(lambda: 0)
        for task in tasks:
            jobs_per_status[task.status] += 1
        jobs_per_status = dict(jobs_per_status)
        with open(run_id + "/tasks/last_status.json", "w") as fd:
            fd.write(json.dumps(jobs_per_status))

    def are_tasks_all_complete(self, tasks):
        is_complete = True
        for task in tasks:
            if task.status in [CREATED, SUBMITTED, UNKNOWN, QUEUED_UNKNOWN, RUNNING]:
                is_complete = False
        return is_complete

    def poll_once(self, run_id, maxsubmit=1000000):
        submitted_count = 0
        while True:
            tasks = self.check_and_print(run_id)

            # persist a snapshot of the current state in a json object
            self.write_json_summary(tasks, run_id)

            active_task_count = len([t for t in tasks if t.status in [SUBMITTED, RUNNING]])
            maxsubmit_now = max(0, maxsubmit - active_task_count)
            created_tasks = [t for t in tasks if t.status == CREATED]
            if len(created_tasks) > maxsubmit_now:
                created_tasks = created_tasks[:maxsubmit_now]

            for task in created_tasks:
                self.job_queue.submit(run_id, task.full_path, "scatter" in task.task_dir)

            submitted_count += len(created_tasks)
            if len(created_tasks) == 0:
                break

        is_complete = self.are_tasks_all_complete(tasks)
        return is_complete, submitted_count

    def poll(self, run_id, wait, maxsubmit):
        is_complete, submitted_count = self.poll_once(run_id, maxsubmit)
        if not is_complete:
            if wait:
                self.wait_for_completion(run_id, maxsubmit)
            else:
                print "Jobs are running, but --nowait was specified, so exiting"

    def kill(self, run_id):
        tasks = self.job_queue.find_tasks(run_id)
        to_kill = [task for task in tasks if task.status in [SUBMITTED, QUEUED_UNKNOWN, RUNNING]]
        self.job_queue.kill(to_kill)
        log.info("%d active jobs killed", len(to_kill))

    def retry(self, run_id, wait, maxsubmit):
        tasks = self.job_queue.find_tasks(run_id)
        for task in tasks:
            if task.status in [FAILED, UNKNOWN]:
                os.unlink("%s/job_id.txt" % task.full_path)
        self.poll(run_id, wait, maxsubmit)


Config = collections.namedtuple("Config", ["base_run_dir", "executor", "invoke", "bsub_options", "qsub_options",
                                           "scatter_bsub_options", "scatter_qsub_options", "workdir", "name", "run_id",
                                           "monitor_port"])


def parse_config(f):
    props = {}
    line_no = 0
    while True:
        line = f.readline()
        line_no += 1

        if line == "":
            break

        # ignore comments
        if line.strip().startswith("#") or len(line.strip()) == 0:
            continue

        # parse prop name and value
        colon_pos = line.find(":")
        if colon_pos < 0:
            raise Exception("Did not find ':' in line %d" % line_no)
        prop_name = line[:colon_pos].strip()
        prop_value = line[colon_pos + 1:].strip()

        # handle quoted values
        if len(prop_value) > 0 and prop_value[0] in ["\"", "'"]:
            if len(prop_value) <= 1 or prop_value[-1] != prop_value[0]:
                raise Exception("Could not find end of quoted string on line %d" % line_no)
            prop_value = prop_value[1:-1].decode("string-escape")

        # check to see if this was "invoke" property, in which case, consume the rest of the file as the script
        # with no escaping
        if prop_name == "invoke":
            prop_value = f.read()

        props[prop_name] = prop_value

    return props


def load_config(filenames, run_id, overrides):
    config = {"bsub_options": "", "qsub_options": "", "workdir": ".", "name": "", "base_run_dir": ".", "monitor_port":None}
    for filename in filenames:
        log.info("Reading config from %s", filename)
        with open(filename) as f:
            config.update(parse_config(f))

    if not ( "scatter_bsub_options" in config ):
        config["scatter_bsub_options"] = config["bsub_options"]

    if not ( "scatter_qsub_options" in config ):
        config["scatter_qsub_options"] = config["qsub_options"]
        # if the scatter qsub options are not set, then default to scatter jobs getting
        # a higher priority.  However, it appears non-admins can't submit jobs with >0 priority
        # so instead, make all other jobs lower priority
        config["qsub_options"] = config["qsub_options"] + " -p -5"

    if not ("run_id" in config):
        config['run_id'] = os.path.abspath(os.path.join(config['base_run_dir'], os.path.basename(run_id)))

    if not ("name" in config):
        # make a hash of the run directory that will likely be unique so that we can distinguish which job is which
        config['name'] = base64.urlsafe_b64encode(hashlib.md5(config['run_id']).digest())[0:10]

    config.update(overrides)

    assert "base_run_dir" in config
    assert "executor" in config
    assert "invoke" in config

    return Config(**config)


def get_flock_home():
    return os.path.dirname(os.path.realpath(__file__))
