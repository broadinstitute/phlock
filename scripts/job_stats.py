# Prints out stats about jobs that matter

import sys
import subprocess
import re

mem_suffix = {"G": 1024*1024*1024, "M": 1024*1024, "K": 1024}

def parse_mem(s):
  last = s[-1]
  scale = 1
  if last in mem_suffix:
    s = s[:-1]
    scale = mem_suffix[last]
  return float(s) * scale

def format_mem(v):
  if v > 1024*1024*1024:
    scale = 1024*1024*1024
    suffix = "G"
  else:
    scale = 1024*1024
    suffix = "M"
  return "%.1f%s" % ((v/scale), suffix)

def format_elapsed(v):
  if v > 60 * 60:
    scale = 60*60
    suffix = " hours"
  else:
    scale = 60
    suffix = " minutes"
  return "%.1f%s" % ((v/scale), suffix)  
  
def print_stats(jobs):
  if len(jobs) == 0:
    print "no jobs"
    return

  runtimes = [float(x['ru_wallclock']) for x in jobs]
  maxvmem = [parse_mem(x['maxvmem']) for x in jobs]
  job_count = len(jobs)
  total_runtime = sum(runtimes)
  mean_runtime = format_elapsed(total_runtime / job_count)
  max_runtime = format_elapsed(max(runtimes))
  mean_maxvmem = format_mem(sum(maxvmem) / job_count)
  max_maxvmem = format_mem(max(maxvmem))
  total_runtime_str = format_elapsed(total_runtime)
  
  print("""# of jobs: {job_count}
total runtime: {total_runtime_str}
mean runtime: {mean_runtime}
max runtime: {max_runtime}
mean maxvmem: {mean_maxvmem}
max maxvmem: {max_maxvmem}
""".format(**locals()))
  

def print_summary(job_pattern):
  stdout = subprocess.check_output(["qacct", "-j", "*"+job_pattern])
  collected = []
  current = {}
  for line in stdout.split("\n"):
    if line.startswith("========"):
      if len(current) > 0:
        collected.append(current)
        current = {}
    elif line != '':
      m = re.match("(\\S+)\\s+(\\S.+)", line)
      assert m != None, "line = %r" % line
      current[m.group(1)] = m.group(2).strip()
  if len(current) > 0:
    collected.append(current)

  print collected[0]

  # now partition into successes and failures
  success = [x for x in collected if x['exit_status'] == '0' and x['failed'] == '0']
  failures = [x for x in collected if x['exit_status'] != '0' or x['failed'] != '0']

  print "success"
  print_stats(success)
  print "failures"
  print_stats(failures)
  

if __name__ == "__main__":
  print_summary(sys.argv[1])
  