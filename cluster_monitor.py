import subprocess
import time
import threading
import traceback
from instance_types import cpus_per_instance
import term
import os

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
C_STOPPED = "stopped"
C_RUNNING = "running"

# different states the cluster manager can be in
CM_RUNNING = "running"
CM_STOPPED = "stopped"

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
    def __init__(self, monitor_parameters, cluster_name, cluster_template, terminal, cmd_prefix, clusterui_identifier, ec2, loadbalance_pid_file):
        super(ClusterManager, self).__init__()
        self.manager_state = CM_STOPPED
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
        self.loadbalance_proc = None
        self.loadbalance_pid_file = loadbalance_pid_file
        import ui
        self.wingman_service = ui.get_wingman_service()

    def start_manager(self):
        # make sure we don't try to have two running manager threads
        assert self.thread is None or not self.thread.is_alive

        self.mailbox.clear()
        # find out if cluster is already running
        self.first_update = True

        self.thread = threading.Thread(target=self.run)
        self.thread.daemon = True
        self.thread.start()

    def stop_manager(self):
        self.mailbox.send("stop-manager")

    def _wait_for(self, messages, timeout=None):
        print "Waiting for %s" % repr(messages)
        msg = self.mailbox.wait_for(messages, timeout=timeout)
        print "Got %s" % repr(msg)
        return msg

    def _main_loop(self):
        running = True
        update_timer = Timer(10)
        while running:
            message = self._wait_for(["stop-manager", "loadbalance-exited"], timeout=update_timer.time_remaining)
            if message == "stop-manager":
                self._execute_shutdown()
                running = False
            elif message == "loadbalance-exited":
                # restart loadbalancer if it exits
                self._execute_startup()
            elif update_timer.expired:
                self._execute_update()
                update_timer.reset()

    def run(self):
        try:
            self.cluster_state = CM_RUNNING
            self._execute_startup()
            self._main_loop()
        except:
            exception_message = traceback.format_exc()

            print(exception_message)
            self.terminal.write(exception_message)

        self.state = CM_STOPPED

    def get_manager_state(self):
        return self.manager_state

    def _run_cmd(self, args, post_execute_msg):
        print "executing %s" % args
        p = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, close_fds=True)
        mp = term.ManagedProcess(p, p.stdout, self.terminal)
        completion = mp.start_thread()
        if post_execute_msg != None:
            completion.then(lambda: self.mailbox.send(post_execute_msg))
        return p

    def _run_starcluster_cmd(self, args, post_execute_msg):
        return self._run_cmd(self.cmd_prefix + args, post_execute_msg)

    def _execute_shutdown(self):
        self.terminal.write("Stopping cluster monitor...\n")
        if self.loadbalance_proc != None:
            self.loadbalance_proc.kill()
            self.loadbalance_proc.wait()

        if os.path.exists(self.loadbalance_pid_file):
            os.unlink(self.loadbalance_pid_file)

    def _execute_startup(self):
        self.terminal.write("Starting cluster monitor...\n")
        if os.path.exists(self.loadbalance_pid_file):
            pid = int(open(self.loadbalance_pid_file).read())
            os.unlink(self.loadbalance_pid_file)
            os.kill(pid)

        self.loadbalance_proc = self._run_starcluster_cmd(["loadbalance", self.cluster_name, "--max_nodes", str(self.monitor_parameters.max_instances),
                                               "--add_nodes_per_iter", str(self.monitor_parameters.max_to_add)], "loadbalance-exited")
        self.manager_state = CM_RUNNING

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
        if not self.monitor_parameters.is_paused:
            print "verifing ownership"
            self._verify_ownership_of_cluster(steal_ownership=self.first_update)
            self.first_update = False

            print "checking terminated nodes"
            self._update_terminated_nodes()
        else:
            print "is paused"

    def start_cluster(self):
        self.terminal.write("Starting cluster...\n")
        self._run_starcluster_cmd(["start", "--cluster-template", self.cluster_template, self.cluster_name], None)

    def stop_cluster(self):
        self.terminal.write("Stopping cluster...\n")
        self._run_starcluster_cmd(["terminate", "-c", "-f", self.cluster_name], None)
