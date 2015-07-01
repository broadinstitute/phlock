__author__ = 'pmontgom'

import collections
import xml.etree.ElementTree as ET
import subprocess
import os
import threading
import time

suffix_scale = {'b': 1, 'G': 1024*1024*1024, 'k': 1024, 'M': 1024*1024}

def parse_mem_size(t):
    if t[-1] in suffix_scale.keys():
        suffix = t[-1]
        t = t[:-1]
    else:
        suffix = 'b'
    return suffix_scale[suffix] * float(t)

def reformat(qhost_output, qstat_output):
    qstat_root = ET.fromstring(qstat_output)

    jobs_by_host = collections.defaultdict(lambda: 0)

    jobs_in_queue = 0

    for job in qstat_root.findall(".//job_list"):
        jobs_in_queue += 1

        #is_running = job.attrib["state"] == "running"
        queue_el = job.find("queue_name")
        if queue_el != None:
            queue_name = queue_el.text
            if queue_name != "" and queue_name != None:
                queue, host = queue_name.split("@")
                jobs_by_host[host] += 1

    hosts = []
    qhost_root = ET.fromstring(qhost_output)
    for host in qhost_root.findall("host"):
        host_name = host.attrib["name"]
        if host_name == "global":
            continue

        host_values = dict(num_proc = None,
            load_avg = None,
            mem_total = None,
            mem_used = None,
            swap_total = None,
            swap_used = None)

        for v in host.findall("hostvalue"):
            name = v.attrib["name"]
            value = v.text

            if name in host_values:
                if value == "-":
                    value = None
                elif name.startswith("mem_") or name.startswith("swap_"):
                    value = parse_mem_size(value)
                else:
                    value = float(value)

                host_values[name] = value

        host_values["name"] = host_name
        host_values["jobs"] = jobs_by_host[host_name]
        #del jobs_by_host[host_name]

        hosts.append( host_values )

    return dict(hosts=hosts, jobs_in_queue=jobs_in_queue)

def get_space_free(path):
    st = os.statvfs(path)
    free = float(st.f_bavail * st.f_frsize)
    return free

class VolumeHistory:
    def __init__(self, window_size=60*60):
        self.per_volume_history = {}
        self.window_size = window_size
        self.lock = threading.Lock()
    
    def update(self, volume):
        # get snapshot of free space
        now = time.time()
        free = get_space_free(volume)

        # prune old entries
        oldest = now - self.window_size
        
        self.lock.acquire()
        if volume in self.per_volume_history:
            prev_history = self.per_volume_history[volume]
        else:
            prev_history = []
        self.lock.release()
        
        history = [ (t, v) for t, v in prev_history if t >= oldest ]
        history.append( (now, free) )

        self.lock.acquire()
        self.per_volume_history[volume] = history
        self.lock.release()

    def _get_monotomic_history(self, history):
        h = [history[0]]
        for i in xrange(1,len(history)):
            if history[i][1] > history[i-1][1]:
                h = []
            h.append(history[i])
        return h
    
    def get_volume_stats(self):
        stats = []
        self.lock.acquire()
        for volume, history in self.per_volume_history.items():
#            print "history", history
            latest_free = history[-1][1]
            history = self._get_monotomic_history(history)
            print "montomic", history
            minutes_until_exhaustion = None

            if len(history) > 1:
                time_delta = history[-1][0] - history[0][0]
                free_delta = history[0][1] - history[-1][1]
                bytes_per_minute = float(free_delta) / ( time_delta / 60.0 )
                print "free_delta", free_delta, "time_delta", time_delta, "byte_per_minute", bytes_per_minute, "latest_free", latest_free
                if bytes_per_minute > 0:
                    minutes_until_exhaustion = latest_free / bytes_per_minute

            stats.append( {"name": volume, "free": latest_free, "minutesUntilExhaustion": minutes_until_exhaustion} )
        self.lock.release()
        return stats

def get_host_summary():
    qhost_output = subprocess.check_output(["qhost", "-xml"])
    qstat_output = subprocess.check_output(["qstat", "-xml"])
    return reformat(qhost_output, qstat_output)


if __name__ == "__main__":
    h = VolumeHistory()
    while True:
        print h.get_volume_stats()
        h.update("/Users")
        time.sleep(10)
    

#qhost_output = open("qhost-output.xml").read()
#qstat_output = open("qstat-output.xml").read()
#print(reformat(qhost_output, qstat_output))