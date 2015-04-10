__author__ = 'pmontgom'

import collections
import xml.etree.ElementTree as ET
import subprocess

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
        queue_el = job.get("queue_name")
        if queue_el != None:
            queue_name = queue_el.text
            if queue_name != "":
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

        hosts.append( host_values )

    return dict(hosts=hosts, jobs_in_queue=jobs_in_queue)

def get_host_summary():
    qhost_output = subprocess.check_output(["qhost", "-xml"])
    qstat_output = subprocess.check_output(["qstat", "-xml"])
    return reformat(qhost_output, qstat_output)

#qhost_output = open("qhost-output.xml").read()
#qstat_output = open("qstat-output.xml").read()
#print(reformat(qhost_output, qstat_output))