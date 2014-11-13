import xmlrpclib
import flock
import logging
import wingman

log = logging.getLogger("flock")

class ConsolidatedMonitor(flock.JobListener):
    def __init__(self, endpoint_url, flock_home):
        self.endpoint_url = endpoint_url
        self.flock_home = flock_home
        self.service = xmlrpclib.ServerProxy(endpoint_url)

        # just make sure we can connect and its working
        version = self.service.get_version()
        assert version != None

    def task_submitted(self, task_dir, external_id):
        flock.JobListener.task_submitted(self, task_dir, external_id)
        self.service.task_submitted(task_dir, external_id)

    def presubmit(self, run_id, task_full_path, task_script, stdout, stderr):
        d = task_full_path
        script_to_execute = "%s/wrapped_task.sh" % d
        with open(script_to_execute, "w") as fd:
            fd.write("set -e\n"
                     "%(notify_command)s started %(run_id)s %(task_dir)s\n"
                     "set +e\n"
                     "if bash %(task_script)s ; then\n"
                     "  set -e\n"
                     "  %(notify_command)s completed %(run_id)s %(task_dir)s\n"
                     "else\n"
                     "  set -e\n"
                     "  %(notify_command)s failed %(run_id)s %(task_dir)s\n"
                     "fi\n" % dict(run_id=run_id,
                                   task_dir=d,
                                   task_script=task_script,
                                   notify_command=wingman.format_notify_command(self.flock_home, self.endpoint_url)))

        return (script_to_execute, stdout, stderr)

import glob
import os

def submit(endpoint_url, run_dir, config_path, name, delete_before_submit):
    service = xmlrpclib.ServerProxy(endpoint_url)

    if delete_before_submit:
        service.delete_run(run_dir)
    service.run_submitted(run_dir, name, config_path, "")

#    for dirname in glob.glob("%s/tasks*" % run_dir):
#        fn = "%s/task_dirs.txt" % dirname
#        if os.path.exists(fn):
#            service.taskset_created(run_dir, fn)

