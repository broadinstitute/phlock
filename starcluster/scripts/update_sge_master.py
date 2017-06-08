#!/usr/bin/python

import subprocess
import tempfile
import sys

defaults_config = """# default memory limit
-l h_vmem=2G
# default memory usage
-l virtual_free=1.7G
"""

#"hostname","load_scaling","complex_values","user_lists","xuser_lists","projects","xprojects","usage_scaling","report_variables"

conf_template="""hostname              %(host)s
load_scaling          NONE
complex_values        virtual_free=%(mem)s,h_vmem=%(mem)s
user_lists            NONE
xuser_lists           NONE
projects              NONE
xprojects             NONE
usage_scaling         NONE
report_variables      NONE"""


def rewrite_line(current, matcher, replacement):
  rewritten = []
  for line in current.split("\n"):
    if matcher(line):
      rewritten.append(replacement)
    else:
      rewritten.append(line)
  return "\n".join(rewritten)

def update_sge_master():
  current = subprocess.check_output(["qconf", "-sc"])
  rewritten = rewrite_line(current, lambda line: line.startswith("virtual_free"), "virtual_free        vf         MEMORY      <=    YES         YES        1G       0")
  rewritten = rewrite_line(rewritten, lambda line: line.startswith("h_vmem"),       "h_vmem              h_vmem     MEMORY      <=    YES         YES        2G       0")

  with tempfile.NamedTemporaryFile() as temp:
    temp.write(rewritten)
    temp.flush()
    
    subprocess.check_call(["qconf", "-Mc", temp.name])
  
  with open("/opt/sge6/default/common/sge_request") as fd:
    b = fd.read()
    
  if not ("# default memory limit" in b):
    with open("/opt/sge6/default/common/sge_request", "w") as fd:
      fd.write(b+defaults_config)
  else:
    print "defaults already set"
    
def update_node(node, memory_size):
  with tempfile.NamedTemporaryFile(delete=False) as temp:
    temp.write(conf_template % {'host': node, "mem": memory_size})
    temp.flush()
    
    subprocess.check_call(["qconf", "-Me", temp.name])

#memory_by_instance_type = {
#  "m3.medium": "3.75G", "m3.large":   "7.5G",  "m3.xlarge":  "15G", "m3.2xlarge": "30G",
#  "c3.large":  "3.75G", "c3.xlarge":  "7.5G",  "c3.2xlarge": "15G", "c3.4xlarge": "30G",  "c3.8xlarge": "60G",
#  "r3.large": "15.25G", "r3.xlarge": "30.5G",  "r3.2xlarge": "61G", "r3.4xlarge": "122G", "r3.8xlarge": "244G"}

memory_by_instance_type = {
  "m1.small": "1.6G", "m1.medium": "3.6G", "m1.large": "7.4G", "m1.xlarge": "14.5G",
  "m3.medium": "3.75G", "m3.large":   "7.5G",  "m3.xlarge":  "15G", "m3.2xlarge": "30G",
  "c3.large":  "3.75G", "c3.xlarge":  "7.5G",  "c3.2xlarge": "15G", "c3.4xlarge": "30G",  "c3.8xlarge": "58G",
  "r3.large": "15.25G", "r3.xlarge": "30.5G",  "r3.2xlarge": "60G", "r3.4xlarge": "122G", "r3.8xlarge": "244G",
  "r4.large": "15.25G", "r4.xlarge": "30.5G",  "r4.2xlarge": "60G", "r4.4xlarge": "122G", "r4.8xlarge": "244G", "r4.16xlarge": "488G"}

update_sge_master()
for i in range(1,len(sys.argv),2):
  node, instance_type = sys.argv[i], sys.argv[i+1]
  update_node(node, memory_by_instance_type[instance_type])
#if sys.argv[1] == "master":
#elif sys.argv[1] == "node":
#  update_sge_node()
