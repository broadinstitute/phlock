import subprocess
import time
import threading
import Queue
import traceback
from instance_types import cpus_per_instance

class Parameters:
    def __init__(self):
        self.interval=30
        self.spot_bid=0.01
        self.max_to_add=5
        self.time_per_job=30 * 60
        self.time_to_add_servers_fixed=60
        self.time_to_add_servers_per_server=30
        self.instance_type="m3.medium"
        self.domain="cluster-deadmans-switch"
        self.dryrun=False
        self.jobs_per_server=1

    def generate_args(self):
        cpus = cpus_per_instance[self.instance_type]

        return ["--spot_bid", str(self.spot_bid * cpus),
                "--max_to_add", str(self.max_to_add),
                "--time_per_job", str(self.time_per_job),
                "--time_to_add_servers_fixed", str(self.time_to_add_servers_fixed),
                "--time_to_add_servers_per_server", str(self.time_to_add_servers_per_server),
                "--instance_type", str(self.instance_type),
                "--domain", self.domain,
                "--jobs_per_server", str(self.jobs_per_server)
                ]



# different states the cluster can be in
CM_STOPPED = "stopped"
CM_STARTING = "starting"
CM_UPDATING = "updating"
CM_SLEEPING = "sleeping"
CM_STOPPING = "stopping"

class ClusterManager(object):
    def __init__(self, monitor_parameters, cluster_name, terminal, cmd_prefix):
        super(ClusterManager, self).__init__()
        self.state = CM_STOPPED
        self.requested_stop = False
        self.monitor_parameters = monitor_parameters
        self.cluster_name = cluster_name
        self.terminal = terminal
        self.queue = Queue.Queue()
        self.cmd_prefix = cmd_prefix

    def _send_wakeup(self):
        self.tell("update")

    def start_manager(self):
        # find out if cluster is already running
        process = subprocess.Popen(self.cmd_prefix+["listclusters", self.cluster_name], stderr=subprocess.PIPE, stdout=subprocess.PIPE)
        output, stderr = process.communicate()

        print "output: %s, stderr=%s" % (repr(output), repr(stderr))

        if "does not exist" in stderr:
            cluster_is_running = False
        else:
            assert "security group" in output
            cluster_is_running = True
            self.state = CM_STARTING

        self.thread = threading.Thread(target=self.run)
        self.thread.daemon = True
        self.thread.start()

        if cluster_is_running:
            self.tell("start-completed")

    def start_cluster(self):
        self.tell("start")

    def stop_cluster(self):
        self.tell("stop")

    def run(self):
        while True:
            try:
                message = self.queue.get()
                self.on_receive(message)
            except:
                traceback.print_exc()

    def tell(self, msg):
        self.queue.put(msg)

    def get_state(self):
        return self.state

    def _run_cmd(self, args, post_execute_msg):
        print "executing %s" % args
        p = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, close_fds=True)
        completion = self.terminal.attach(p.stdout, lambda: self.terminal.run_until_terminate(p), p)
        completion.then(lambda: self.tell(post_execute_msg))

    def _run_starcluster_cmd(self, args, post_execute_msg):
        self._run_cmd(self.cmd_prefix + args, post_execute_msg)

    def _execute_shutdown(self):
        self._run_starcluster_cmd(["terminate", "--confirm", self.cluster_name], "stop-completed")
        self.state = CM_STOPPING

    def _execute_startup(self):
        cmd, flag, config = self.cmd_prefix
        assert flag == "-c"
        self._run_cmd(["./start_cluster.sh", cmd, config, self.cluster_name], "start-completed")
        self.state = CM_STARTING

    def _execute_sleep_then_poll(self):
        self.state = CM_SLEEPING
        sleep_timer = threading.Timer(self.monitor_parameters.interval, self._send_wakeup)
        sleep_timer.start()

    def _execute_poll(self):
        self.state = CM_UPDATING
        args = self.monitor_parameters.generate_args()
        self._run_starcluster_cmd(["scalecluster", self.cluster_name] + args, "update-completed")

    def on_receive(self, message):
        if isinstance(message, dict):
            cmd = message['command']
        else:
            cmd = message

        print "on_receive(%s)" % cmd

        if cmd == "state?":
            return self.state
        elif cmd == "start":
            if self.state == CM_STOPPED:
                self._execute_startup()
        elif cmd == "stop":
            if self.state in [CM_STARTING, CM_STOPPING, CM_UPDATING]:
                self.requested_stop = True
            elif self.state in [CM_SLEEPING, CM_STOPPED]:
                print("xecut schut")
                self._execute_shutdown()
            else:
                print "stop but state = %s" % self.state
        elif cmd == "update":
            if self.state == CM_SLEEPING:
                self._execute_poll()
        elif cmd == "update-completed":
            if self.state == CM_UPDATING:
                self._execute_sleep_then_poll()
        elif cmd == "start-completed":
            if self.state == CM_STARTING:
                self._execute_sleep_then_poll()
        elif cmd == "stop-completed":
            if self.state == CM_STOPPING:
                self.state = CM_STOPPED


