import subprocess
import time
import threading
import Queue
import traceback
from instance_types import cpus_per_instance
import term

class Parameters:
    def __init__(self):
        self.is_paused=True
        self.interval=30
        self.spot_bid=0.01
        self.max_to_add=1
        self.time_per_job=30 * 60
        self.time_to_add_servers_fixed=60
        self.time_to_add_servers_per_server=30
        self.max_instances=10
        self.instance_type="m3.medium"
        self.domain="cluster-deadmans-switch"
        self.dryrun=False
        self.jobs_per_server=1
        self.log_file = None

    def generate_args(self):
        cpus = cpus_per_instance[self.instance_type]

        return ["--spot_bid", str(self.spot_bid * cpus),
                "--max_to_add", str(self.max_to_add),
                "--time_per_job", str(self.time_per_job),
                "--time_to_add_servers_fixed", str(self.time_to_add_servers_fixed),
                "--time_to_add_servers_per_server", str(self.time_to_add_servers_per_server),
                "--instance_type", str(self.instance_type),
                "--domain", self.domain,
                "--jobs_per_server", str(self.jobs_per_server),
                "--logfile", self.log_file,
                "--max_instances", str(self.max_instances)
                ]

# different states the cluster can be in
CM_STOPPED = "stopped"
CM_STARTING = "starting"
CM_UPDATING = "updating"
CM_SLEEPING = "sleeping"
CM_STOPPING = "stopping"
CM_DEAD = "dead"

class Timer(object):
    def __init__(self, interval):
        self.interval = interval
        self.next_expiry = None
        self.reset()

    @property
    def time_remaining(self):
        now = time.time()
        remaining = self.next_expiry - now
        return max(0, remaining)

    @property
    def expired(self):
        return self.time_remaining == 0

    def reset(self):
        self.next_expiry = time.time() + self.interval

class Mailbox(object):
    def __init__(self):
        self.queue = []
        self.cv = threading.Condition()

    def wait_for(self, messages, timeout=None):
        if timeout != None:
            timer = Timer(timeout)
        else:
            timer = None

        self.cv.acquire()
        result = None
        while True:
            for i in range(len(self.queue)):
                if self.queue[i] in messages:
                    result = self.queue[i]
                    del self.queue[i]
                    break
            if result != None:
                break

            if timer != None:
                time_remaining = timer.time_remaining
                print "Time remaining %s" % time_remaining
                if time_remaining > 0:
                    self.cv.wait(time_remaining)
                else:
                    break
            else:
                self.cv.wait()

        self.cv.release()
        return result

    def send(self, message):
        print "sending %s" % repr(message)
        self.cv.acquire()
        self.queue.append(message)
        self.cv.notify_all()
        self.cv.release()

    def clear(self):
        self.cv.acquire()
        self.queue = []
        self.cv.release()

class ClusterManager(object):
    def __init__(self, monitor_parameters, cluster_name, cluster_template, terminal, cmd_prefix, clusterui_identifier, ec2):
        super(ClusterManager, self).__init__()
        self.state = CM_DEAD
        self.requested_stop = False
        self.monitor_parameters = monitor_parameters
        self.cluster_name = cluster_name
        self.terminal = terminal
        self.cmd_prefix = cmd_prefix
        self.mailbox = Mailbox()
        self.clusterui_identifier = clusterui_identifier
        self.ec2 = ec2
        self.cluster_template = cluster_template
        self.first_update = True
        self.thread = None
        self.terminated_nodes = set()
        import ui
        self.wingman_service = ui.get_wingman_service()

    def start_manager(self):
        # make sure we don't try to have two running manager threads
        assert self.thread is None or not self.thread.is_alive

        # find out if cluster is already running
        process = subprocess.Popen(self.cmd_prefix+["listclusters", self.cluster_name], stderr=subprocess.PIPE, stdout=subprocess.PIPE)
        output, stderr = process.communicate()

        print "output: %s, stderr=%s" % (repr(output), repr(stderr))

        if "does not exist" in stderr:
            cluster_is_running = False
            self.state = CM_STOPPED
        else:
            assert "security group" in output
            cluster_is_running = True
            self.state = CM_SLEEPING
            self.first_update = True

        self.thread = threading.Thread(target=self.run)
        self.thread.daemon = True
        self.thread.start()

        if cluster_is_running:
            self.mailbox.send("start-completed")

    def start_cluster(self):
        self.mailbox.send("start")

    def stop_cluster(self):
        self.mailbox.send("stop")

    def _wait_for(self, messages, timeout=None):
        print "Waiting for %s" % repr(messages)
        msg = self.mailbox.wait_for(messages, timeout=timeout)
        print "Got %s" % repr(msg)
        return msg

    def _main_loop(self):
        while True:
            if self.state == CM_STOPPED:
                self._wait_for(["start"])
                self.state = CM_STARTING
                self._execute_startup()

            running = True
            update_timer = Timer(60)
            while running:
                message = self._wait_for(["shutdown"], timeout=update_timer.time_remaining)
                if message == "shutdown":
                    self._execute_shutdown()
                    running = False
                elif update_timer.expired:
                    self._execute_update()
                    update_timer.reset()

    def run(self):
        try:
            self._main_loop()
        except:
            exception_message = traceback.format_exc()

            print(exception_message)
            self.terminal.write(exception_message)

        self.state = CM_DEAD

    def get_state(self):
        return self.state

    def _run_cmd(self, args, post_execute_msg):
        print "executing %s" % args
        p = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, close_fds=True)
        mp = term.ManagedProcess(p, p.stdout, self.terminal)
        completion = mp.start_thread()
        completion.then(lambda: self.mailbox.send(post_execute_msg))

    def _run_starcluster_cmd(self, args, post_execute_msg):
        self._run_cmd(self.cmd_prefix + args, post_execute_msg)

    def _execute_shutdown(self):
        self._run_starcluster_cmd(["terminate", "-f", "--confirm", self.cluster_name], "stop-completed")
        self.state = CM_STOPPING
        self._wait_for(["stop-completed"])

    def _execute_startup(self):
        cmd, flag, config = self.cmd_prefix
        assert flag == "-c"
        self._run_cmd(["./start_cluster.sh", cmd, config, self.cluster_name, self.cluster_template], "start-completed")
        self.state = CM_STARTING
        self._wait_for(["start-completed"])

    def _verify_ownership_of_cluster(self, steal_ownership=False):
        security_group_name = "@sc-%s" % self.cluster_name
        security_groups = self.ec2.get_all_security_groups([security_group_name])
        #if len(security_groups) == 0:
        #    return

        security_group_id = security_groups[0].id

        tags = self.ec2.get_all_tags(filters={"resource-id": security_group_id, "key": "clusterui-instance"})
        if len(tags) == 0 or steal_ownership:
            self.ec2.create_tags([security_group_id], {"clusterui-instance": self.clusterui_identifier})
        else:
            assert len(tags) == 1
            tag = tags[0]
            if tag.value != self.clusterui_identifier:
                self.state = "broken-lost-ownership"
                raise Exception("Expected ownership tag to be %s but was %s" % (repr(self.clusterui_identifier, tag.value)))

    def _update_terminated_nodes(self):
        import ui
        instances = ui.find_terminated_in_cluster(self.ec2, self.cluster_name)
        aliases = set()
        for instance in instances:
            if "alias" in instance.tags:
                aliases.add(instances.tags["alias"])
        # find the aliases which have been terminated since we last checked
        newly_terminated = aliases - self.terminated_nodes
        # remember what nodes were terminated on this pass
        self.terminated_nodes = aliases

        print "Terminated nodes: %s, new: %s" % (self.terminated_nodes, newly_terminated)
        for alias in newly_terminated:
            self.wingman_service.node_disappeared(alias)

    def _execute_update(self):
        self.state = CM_UPDATING
        if not self.monitor_parameters.is_paused:
            print "verifing ownership"
            self._verify_ownership_of_cluster(steal_ownership=self.first_update)
            self.first_update = False

            print "checking terminated nodes"
            self._update_terminated_nodes()

            print "calling scalecluster"
            args = self.monitor_parameters.generate_args()
            self._run_starcluster_cmd(["scalecluster", self.cluster_name] + args, "update-completed")
            self._wait_for(["update-completed"])
        else:
            print "is paused"

