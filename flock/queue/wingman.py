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
