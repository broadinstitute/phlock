import sys
import logging
import __init__ as flock
import os
import xmlrpclib

__author__ = 'pmontgom'

class MonitorQueue(flock.AbstractQueue):
    def __init__(self, listener, monitor, run_id):
        super(MonitorQueue, self).__init__(listener)
        self.monitor = monitor
        self.run_id = run_id
        self.external_id_prefix = "Invalid"

    def get_jobs_from_external_queue(self):
        return {}

    def add_to_queue(self, task_full_path, is_scatter, script_to_execute, stdout_path, stderr_path):
        self.monitor.task_created(task_full_path, self.run_id)

    def kill(self, task):
        raise Exception("unimplemented")

def main():
    FORMAT = "[%(asctime)-15s] %(message)s"
    logging.basicConfig(format=FORMAT, level=logging.INFO, datefmt="%Y%m%d-%H%M%S")

    flock_home = os.path.dirname(os.path.realpath(__file__))

    endpoint_url = sys.argv[1]
    monitor = xmlrpclib.ServerProxy(endpoint_url)
    config_paths = sys.argv[2:]

    listener = flock.ConsolidatedMonitor(endpoint_url, flock_home)

    for config_path in config_paths:
        config = flock.load_config([config_path], None, {})

        monitor.run_created(config.run_id, config.name, config_path, "")

        queue = MonitorQueue(listener, monitor, config.run_id)
        f = flock.Flock(queue, flock_home, notify_command="python %s/notify.py %s" % (flock_home, endpoint_url))
        f.run(config.run_id, config.invoke, False, maxsubmit=1e6, test_job_count=None, no_poll=True)

        # probably better to fold this logic into a callback for f.run() somehow, but for now do it as a
        # check afterwards
        task_definition_path = ("%s/tasks-init/task_dirs.txt" % config.run_id)
        assert os.path.exists(task_definition_path)

        monitor.taskset_created(config.run_id, task_definition_path)


if __name__ == "__main__":
    main()
