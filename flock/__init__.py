import argparse
import sys
import collections
import os
import glob
import subprocess
import time
import logging
import json

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
    if run_id != None:
        task_dir = os.path.join(run_id, task_dir)
    finished = os.path.exists(os.path.join(task_dir, "finished-time.txt"))
    return finished




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

def get_external_id(run_id, task_dir):
    job_id_file = "%s/%s/job_id.txt" % (run_id, task_dir)
    if os.path.exists(job_id_file):
        with open(job_id_file) as fd:
            job_id = fd.read()
            return job_id
    return None

@timeit
def read_external_ids(run_id, task_dirs, expected_prefix):
    external_ids = collections.defaultdict(lambda: [])
    for task_dir in task_dirs:
        job_id = get_external_id(run_id, task_dir)
        if job_id != None:
            assert job_id.startswith(expected_prefix), "Job ID was expected to be %s but was %s" % (expected_prefix, job_id)
            external_ids[task_dir] = job_id[len(expected_prefix):]
    return external_ids



class JobListener(object):
    def get_notify_command(self):
        return None

    def task_submitted(self, task_dir, external_id):
        with open("%s/job_id.txt" % task_dir, "w") as fd:
            fd.write(external_id)

    def presubmit(self, run_id, task_full_path, task_script, stdout, stderr):
        return (task_script, stdout, stderr)









def dump_file(filename):
    if os.path.exists(filename):
        with open(filename) as fd:
            for line in fd.readlines():
                sys.stdout.write("  ")
                sys.stdout.write(line)
    else:
        sys.stdout.write("  [ File %s does not exist ]" % filename)

def write_python_scatter_script(run_id, test_job_count, flock_home, notify_command, script_body):
    run_dir = os.path.abspath(run_id)
    temp_run_script = "%s/tasks-init/scatter/scatter.R" % run_id
    with open(temp_run_script, "w") as fd:
        fd.write("flock_settings = dict(flock_starting_file='%s/tasks-init/scatter/started-time.txt',\n" % run_dir)
        fd.write("  flock_completion_file='%s/tasks-init/scatter/finished-time.txt',\n" % run_dir)
        fd.write("  flock_test_job_count=%s,\n" % (repr(test_job_count)))
        fd.write("  flock_version=%s,\n" % repr(FLOCK_VERSION.split(".")))
        fd.write("  flock_run_dir='%s',\n" % (run_dir))
        fd.write("  flock_home='%s',\n" % (flock_home))
        fd.write("  flock_notify_command=%s)\n" % repr(notify_command))

        fd.write("with open(flock_starting_file, 'w') as fd:\n"
                 "  fd.write(format(Sys.time(), '%a %b %d %X %Y'))\n")

        fd.write("execfile('%s/flock_support.R');\n" % flock_home)
        fd.write(script_body)
        fd.write("# write out record that task completed successfully\n"
                 "with open(flock_completion_file, 'w') as fd:\n"
                 "  fd.write(format(Sys.time(), '%a %b %d %X %Y'), fileConn)\n")
    return temp_run_script


def write_r_scatter_script(run_id, test_job_count, flock_home, notify_command, script_body):
    run_dir = os.path.abspath(run_id)
    temp_run_script = "%s/tasks-init/scatter/scatter.R" % run_id
    with open(temp_run_script, "w") as fd:
        fd.write("flock_starting_file <- '%s/tasks-init/scatter/started-time.txt'\n" % run_dir)
        fd.write("flock_completion_file <- '%s/tasks-init/scatter/finished-time.txt'\n" % run_dir)
        fd.write("flock_test_job_count <- %s\n" % ("NA" if test_job_count == None else test_job_count))
        fd.write("flock_version <- c(%s);\n" % ", ".join(FLOCK_VERSION.split(".")))
        fd.write("flock_run_dir <- '%s';\n" % (run_dir))
        fd.write("flock_home <- '%s';\n" % (flock_home))
        if notify_command:
            fd.write("flock_notify_command <- '%s';\n" % notify_command)
        else:
            fd.write("flock_notify_command <- NULL;\n")

        fd.write("""fileConn<-file(flock_starting_file)
        writeLines(format(Sys.time(), "%a %b %d %X %Y"), fileConn)
        close(fileConn)
        """)

        fd.write("source('%s/flock_support.R');\n" % flock_home)
        fd.write(script_body)
        fd.write("""# write out record that task completed successfully
        fileConn<-file(flock_completion_file)
        writeLines(format(Sys.time(), "%a %b %d %X %Y"), fileConn)
        close(fileConn)
        """)
    return temp_run_script

def write_files_for_running(flock_home, notify_command, run_id, script_body, test_job_count, environment_variables, language):
    run_dir = os.path.abspath(run_id)
    if os.path.exists(run_id):
        raise Exception("\"%s\" already exists. Aborting.", run_id)

    os.makedirs("%s/temp" % run_id)
    os.makedirs("%s/tasks-init/scatter" % run_id)
    os.makedirs("%s/tasks" % run_id)
    environment_script = "%s/env.sh" % run_id

    with open(environment_script, "w") as fd:
        for stmt in environment_variables:
            fd.write("export %s\n" % (stmt))

    if language == "R":
        temp_run_script = write_r_scatter_script(run_id, test_job_count, flock_home, notify_command, script_body)
    elif language == "python":
        temp_run_script = write_python_scatter_script(run_id, test_job_count, flock_home, notify_command, script_body)
    else:
        raise Exception("Unknown language: %s" % language)

    #TODO: use default, but support override
    python_path = "python"

    with open("%s/tasks-init/scatter/task.sh" % run_id, "w") as fd:
        fd.write("source %s\n" % environment_script)
        if language == "R":
            fd.write("exec R --vanilla < %s\n" % temp_run_script)
        elif language == "python":
            fd.write("exec %s %s" % (python_path, temp_run_script))
        else:
            raise Exception("Unknown language: %s" % language)

    task_definition_path = "%s/tasks-init/task_dirs.txt" % run_id
    with open(task_definition_path, "w") as fd:
        fd.write("1 tasks-init/scatter\n")

    return task_definition_path

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

    def run(self, run_id, script_body, wait, maxsubmit, test_job_count, environment_variables, language, no_poll=False):
        write_files_for_running(self.flock_home, self.notify_command, run_id, script_body, test_job_count, environment_variables, language)

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

def get_flock_home():
    return os.path.dirname(os.path.realpath(__file__))
