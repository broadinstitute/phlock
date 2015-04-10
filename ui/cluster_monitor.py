import subprocess
import time
import threading
import traceback
from instance_types import cpus_per_instance
import term
import os
import socket
import logging
log = logging.getLogger("ui")

class Parameters:
    def __init__(self, default_instance_type, ignore_grp):
        self.interval=30
        self.spot_bid=0.01
        self.max_to_add=1
        self.max_instances=10
        self.min_instances=1
        self.instance_type=default_instance_type
        self.dryrun=False
        self.log_file = None
        self.job_wait_time = 60
        self.stabilization_time = 60
        self.ignore_grp = ignore_grp
        self.reserve_node_timeout = 60
        self.reserve_nodes = 1

    def get_total_spot_bid(self):
        cpus = cpus_per_instance[self.instance_type]
        return self.spot_bid * cpus


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
                log.debug("Time remaining %s",time_remaining)
                if time_remaining > 0:
                    self.cv.wait(time_remaining)
                else:
                    break
            else:
                self.cv.wait()

        self.cv.release()
        return result

    def send(self, message):
        log.info("sending %s",repr(message))
        self.cv.acquire()
        self.queue.append(message)
        self.cv.notify_all()
        self.cv.release()

    def clear(self):
        self.cv.acquire()
        self.queue = []
        self.cv.release()

class ClusterManager(object):
    def __init__(self, monitor_parameters, cluster_name, cluster_template, terminal, cmd_prefix, clusterui_identifier, ec2, loadbalance_pid_file, sdbc, wingman_service_factory):
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
        self.instance_id_to_alias = {}
        self.loadbalance_proc = None
        self.loadbalance_pid_file = loadbalance_pid_file
        self.loadbalance_start_time = None
        self.sdbc = sdbc
        self.restart_times = []
        self.wingman_service_factory = wingman_service_factory

    def start_manager(self):
        # make sure we don't try to have two running manager threads
        assert self.thread is None or not self.thread.is_alive()

        self.mailbox.clear()
        # find out if cluster is already running
        self.first_update = True

        self.thread = threading.Thread(target=self.run)
        self.thread.daemon = True
        self.thread.start()

    def stop_manager(self):
        self.mailbox.send("stop-manager")

    def _wait_for(self, messages, timeout=None):
        log.debug("Waiting for %s", repr(messages))
        msg = self.mailbox.wait_for(messages, timeout=timeout)
        log.debug("Got %s", repr(msg))
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
                self.terminal.write("loadbalancer terminated unexpectedly\n")
                # when the loadbalancer exits, we should auto-restart,
                # but keep track of the times of the last 5 restarts so that detect whether the process is flapping
                now = time.time()
                self.restart_times.append(now)
                self.terminal.write("termination times: %s\n"%repr(self.restart_times))
                if len(self.restart_times) > 5:
                    self.restart_times = self.restart_times[-5:]
                    if self.restart_times[0] > now - (60*30):
                        raise Exception("Loadbalancer has restarted 5 times in less than 30 minutes.  There may be a problem with the loadbalancer.")

                # if we didn't throw an exception, restart the loadbalancer
                self._execute_startup()

            elif update_timer.expired:
                self._execute_update()
                update_timer.reset()

    def run(self):
        try:
            self.manager_state = CM_RUNNING
            self._execute_startup()
            self._main_loop()
        except:
            exception_message = traceback.format_exc()

            log.exception("Got exception in run()")
            self.terminal.write(exception_message)
            self._kill_loadbalance_proc()

        self.terminal.write("Cluster monitor terminated\n")
        self.manager_state = CM_STOPPED

    def get_manager_state(self):
        return self.manager_state

    def _run_cmd(self, args, post_execute_msg, completion_callback=None):
        log.info("executing %s", args)
        p = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, close_fds=True)
        mp = term.ManagedProcess(p, p.stdout, self.terminal)
        completion = mp.start_thread()
        if post_execute_msg != None:
            completion.then(lambda: self.mailbox.send(post_execute_msg))
        if completion_callback != None:
            completion.then(completion_callback)
        return p

    def _run_starcluster_cmd(self, args, post_execute_msg, completion_callback=None):
        return self._run_cmd(self.cmd_prefix + args, post_execute_msg, completion_callback=completion_callback)

    def _kill_loadbalance_proc(self):
        if self.loadbalance_proc != None:
            self.terminal.write("killing loadbalance process\n")
            try:
                self.loadbalance_proc.kill()
                self.loadbalance_proc.wait()
            except OSError:
                pass # swallow exception if pid does not exist

        if os.path.exists(self.loadbalance_pid_file):
            pid = int(open(self.loadbalance_pid_file).read())
            os.unlink(self.loadbalance_pid_file)
            try:
                os.kill(pid)
            except OSError:
                pass # swallow exception if pid does not exist

    def _execute_shutdown(self):
        self.terminal.write("Stopping cluster monitor...\n")
        self._kill_loadbalance_proc()

    def _execute_startup(self):
        self.loadbalance_start_time = time.time()
        self.terminal.write("Starting cluster monitor...\n")
        self._kill_loadbalance_proc()

        args = ["--max_nodes", str(self.monitor_parameters.max_instances),
                "--add_nodes_per_iter", str(self.monitor_parameters.max_to_add),
                "--spot-bid", str(self.monitor_parameters.get_total_spot_bid()),
                "--min_nodes", str(self.monitor_parameters.min_instances),
                "--instance-type", self.monitor_parameters.instance_type,
                "--job_wait_time", str(self.monitor_parameters.job_wait_time),
                "--stabilization_time", str(self.monitor_parameters.stabilization_time),
                "--reserve_node_timeout", str(self.monitor_parameters.reserve_node_timeout),
                "--reserve_nodes", str(self.monitor_parameters.reserve_nodes)]
        if self.monitor_parameters.ignore_grp:
            args.append("--ignore-grp")
        self.loadbalance_proc = self._run_starcluster_cmd(["loadbalance", self.cluster_name] + args, "loadbalance-exited")

    def _verify_ownership_of_cluster(self, steal_ownership=False):
        security_group_name = "@sc-%s" % self.cluster_name
        security_groups = self.ec2.get_all_security_groups([security_group_name])

        security_group_id = security_groups[0].id

        tags = self.ec2.get_all_tags(filters={"resource-id": security_group_id, "key": "clusterui-instance"})
        if len(tags) == 0 or steal_ownership:
            self.ec2.create_tags([security_group_id], {"clusterui-instance": self.clusterui_identifier})
        else:
            assert len(tags) == 1
            tag = tags[0]
            if tag.value != self.clusterui_identifier:
                self.state = "broken-lost-ownership"
                raise Exception("Expected ownership tag to be %s but was %s" % (repr(self.clusterui_identifier), repr(tag.value)))

    def _update_terminated_nodes(self):
        import ui

        # update instance_id_to_alias with running instances that have an alias set
        instances = ui.find_instances_in_cluster(self.ec2, self.cluster_name)
        for instance in instances:
            if "alias" in instance.tags:
                alias = instance.tags["alias"]
                self.instance_id_to_alias[instance.id] = alias

        newly_terminated_aliases = set()
        newly_terminated_ids = set()

        # look up each terminated instance's id to see if it was running previously
        terminated_instances = ui.find_terminated_in_cluster(self.ec2)
        for instance in terminated_instances:
            if instance.id in self.instance_id_to_alias:
                alias = self.instance_id_to_alias[instance.id]
                newly_terminated_aliases.add(alias)
                newly_terminated_ids.add(instance.id)

        wingman_service = self.wingman_service_factory()
        log.info("Terminated nodes: %s, new: %s", terminated_instances, newly_terminated_aliases)
        for alias in newly_terminated_aliases:
            wingman_service.node_disappeared(alias)

        # forget about these id before the next check
        for id in newly_terminated_ids:
            del self.instance_id_to_alias[id]


    def _send_heartbeat(self):
        domain = "%s-heartbeats" % self.cluster_name

        dom = self.sdbc.lookup(domain)
        assert dom != None
        self.terminal.write("sending heartbeat to domain %s\n" % domain)
        dom.put_attributes('heartbeat', {'timestamp': time.time(), 'hostname': socket.getfqdn()})

    def _execute_update(self):
        if self.loadbalance_start_time != None and ((time.time() - self.loadbalance_start_time) > 5*60):
            if self.loadbalance_proc.poll() == None:
                # only if the loadbalancer has been up for > 5 minutes do we really think its running
                self._send_heartbeat()

        log.debug("verifing ownership")
        self._verify_ownership_of_cluster(steal_ownership=self.first_update)
        self.first_update = False

        print "checking terminated nodes"
        self._update_terminated_nodes()

    def start_cluster(self):
        self.terminal.write("Starting cluster...\n")
        self._run_starcluster_cmd(["start", "--cluster-template", self.cluster_template, self.cluster_name, "--config-on-master"], None, completion_callback=lambda: self.terminal.write("Cluster started\n"))

    def stop_cluster(self):
        self.terminal.write("Stopping cluster...\n")
        self._run_starcluster_cmd(["terminate", "-c", "-f", self.cluster_name], None, completion_callback=lambda: self.terminal.write("Cluster stopped\n"))
