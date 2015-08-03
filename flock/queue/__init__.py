import collections
import flock
import os
import time

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
        self.record_finished_count(time.time(), counts[flock.FINISHED] + counts[flock.FAILED] + counts[flock.UNKNOWN])
        return self.estimate_completion_rate(
            counts[flock.SUBMITTED] + counts[flock.WAITING] + counts[flock.RUNNING] + counts[flock.QUEUED_UNKNOWN])

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
        if not (task_dir in self.finished_successfully) and flock.finished_successfully(run_id, task_dir):
            self.finished_successfully.add(task_dir)
        return task_dir in self.finished_successfully

    def get_status(self, run_id, external_ids, queued_job_states, task_dir, job_deps):
        assert type(queued_job_states) == dict

        if self._finished_successfully(run_id, task_dir):
            return flock.FINISHED

        if task_dir in external_ids:
            lsf_id = external_ids[task_dir]
            if lsf_id in queued_job_states:
                self.update_failure(task_dir, True)
                return queued_job_states[lsf_id]
            else:
                self.update_failure(task_dir, False)
                if self.definitely_failed(task_dir):
                    return flock.FAILED
                else:
                    return flock.UNKNOWN
        else:
            all_deps_met = True
            for dep in job_deps[task_dir]:
                if self.get_status(run_id, external_ids, queued_job_states, dep, job_deps) != flock.FINISHED:
                    all_deps_met = False
            if all_deps_met:
                return flock.CREATED
            else:
                return flock.WAITING

class AbstractQueue(object):
    def __init__(self, listener):
        self.cache = TaskStatusCache()
        self.last_estimate = None
        self.listener = listener

    def submit(self, run_id, task_full_path, task_type):
        self.clean_task_dir(task_full_path)
        d = task_full_path

        stdout = "%s/stdout.txt" % d
        stderr = "%s/stderr.txt" % d
        script_to_execute = "%s/task.sh" % d

        script_to_execute, stdout, stderr = self.listener.presubmit(run_id, task_full_path, script_to_execute, stdout, stderr)

        self.add_to_queue(task_full_path, task_type, script_to_execute, stdout, stderr)

    def get_last_estimate(self):
        return self.last_estimate

    def find_tasks(self, run_id):
        task_dirs, job_deps = flock.read_task_dirs(run_id)
        queued_job_states = self.get_jobs_from_external_queue()
        external_ids = flock.read_external_ids(run_id, task_dirs, self.external_id_prefix)
        tasks = flock.find_tasks(run_id, external_ids, queued_job_states, task_dirs, job_deps, self.cache)
        self.last_estimate = self.cache.update_estimate(tasks)
        return tasks

    def clean_task_dir(self, task_full_path):
        clean_task_dir(task_full_path)

def clean_task_dir(task_full_path):
    for fn in ["%s/stdout.txt" % task_full_path, "%s/stderr.txt" % task_full_path]:
        if os.path.exists(fn):
            for i in xrange(20):
                dest = "%s-%d.txt" % (fn[:-4], i+1)
                if not os.path.exists(dest):
                    break
            os.rename(fn, dest)
