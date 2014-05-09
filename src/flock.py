import argparse
import sys
import collections
import os
import csv
import glob
import subprocess
import re

# the various status codes for tasks
CREATED = "Created"
SUBMITTED = "Submitted"
FINISHED = "Finished"
WAITING = "Waiting for other jobs to finish"

Task = collections.namedtuple("Task", ["task_dir", "external_id", "status", "full_path"])

def system(cmd):
  print "EXEC %s:" % repr(cmd)
  retcode = subprocess.call(cmd, env=modified_env, shell=True)
  if retcode != 0:
    raise Exception("Command terminated with exit status = %d" % retcode)
  #os.system(cmd)

def read_task_dirs(run_id):
  """ returns a tuple of (task_dirs, job_deps) where 
      task_dirs is a list of task directories
      job_deps is a map of task_dir -> set of tasks that must complete before this can start
  """
  task_dirs = []
  job_deps = collections.defaultdict(lambda:set())
  for dirname in glob.glob("%s/job*" % run_id):
    fn = "%s/task_dirs.txt" % dirname
    if os.path.exists(fn):
      grouped_commands = {}
      with open(fn) as fd:
        for line in fd.readlines():
          line = line.strip()
          i=line.find(" ")
          group = int(line[:i])
          command = line[i+1:]
          
          if group in grouped_commands:
            commands = grouped_commands[group]
          else:
            commands = []
            grouped_commands[group] = commands
          
          commands.append(command)
      # now, we'll assume we have two groups: 1 and 2
      # where 1 is all the scatter jobs, and 2 is the gather.
      # (In practice that's what happens)
      scatter_tasks = grouped_commands[1]
      gather_tasks = grouped_commands[2]
      task_dirs.extend(scatter_tasks)
      task_dirs.extend(gather_tasks)
      for gather_task in gather_tasks:
        job_deps[gather_task] = set(scatter_tasks)
  return (task_dirs, job_deps)  

def is_finished(run_id, task_dir):
  finished = os.path.exists("%s/%s/finished-time.txt" % (run_id, task_dir))
  #print "is_finished %s/finished-time.txt -> %s" % (task_dir, finished)
  return finished

class LocalQueue(object):
  def __init__(self):
    self._ran = set()
  
  def find_tasks(self, run_id):
    task_dirs, task_deps = read_task_dirs(run_id)
    tasks = []
    tasks.extend([ Task(task_dir, None, FINISHED if is_finished(run_id, task_dir) else CREATED, run_id+"/"+task_dir) for task_dir in task_dirs ])
    return tasks

  def submit(self, task):
    d = task.full_path
    cmd = "bash %s/task.sh > %s/stdout.txt 2> %s/stderr.txt" % (d,d,d)
    if cmd in self._ran:
      raise Exception("Already ran %s once" % cmd)
    system(cmd)
    self._ran.add(cmd)
    
  def kill(self, task):
    raise Exception("not implemented")

class LsfQueue(object):
  def get_active_lsf_jobs(self):
    handle = subprocess.Popen(["bjobs", "-w"], stdout=subprocess.PIPE)
    stdout, stderr = handle.communicate()

    #Output looks like:
    #  JOBID   USER    STAT  QUEUE      FROM_HOST   EXEC_HOST   JOB_NAME   SUBMIT_TIME
    #  6265422 pmontgo PEND  bhour      tin                     *h -c echo May  9 17:11
    # or
    #  No unfinished job found
    lines = stdout.split("\n")
    job_pattern = re.compile("\\s*(\\d+)\\s+\\S+\\s+(\\S+)\\s+.*")
    active_jobs = set()
    for line in lines[1:]:
      if line == '':
        continue
      m = job_pattern.match(line)
      if m == None:
        print "Could not parse line from bjobs: %s" % repr(line)
      else:
        job_id = m.group(1)
        job_state = m.group(2)
        active_jobs.add(job_id)
    return active_jobs
    
  def find_tasks(self, run_id):
    active_lsf_jobs = self.get_active_lsf_jobs()
    task_dirs, job_deps = read_task_dirs(run_id)
    tasks = []

    external_ids = {}
    for task_dir in task_dirs:
      job_id_file = "%s/%s/lsf_job_id.txt" % (run_id, task_dir)
      print "Checking %s: %s" % (job_id_file, os.path.exists(job_id_file))
      if os.path.exists(job_id_file):
        with open(job_id_file) as fd:
          external_ids[task_dir] = fd.read()

    def get_external_id(task_dir):
      if task_dir in external_ids:
        return external_ids[task_dir]
      else:
        return None

    def get_status(task_dir):
      if task_dir in external_ids:
        lsf_id = get_external_id(task_dir)
        if lsf_id in active_lsf_jobs:
          return SUBMITTED
        else:
          return FINISHED
      else:
        all_deps_met = True
        for dep in job_deps[task_dir]:
          if get_status(dep) != FINISHED:
            all_deps_met = False
        if all_deps_met:
          return CREATED
        else:
          return WAITING
        
    #for task_dirs in grouped_dirs:
    tasks.extend([ Task(task_dir, get_external_id(task_dir), get_status(task_dir), run_id+"/"+task_dir) for task_dir in task_dirs ])

    return tasks
    
  def submit(self, task):
    d = task.full_path
    cmd = ["bsub", "-o", "%s/stdout.txt" % d, "-e", "%s/stderr.txt" % d, "bash %s/task.sh" % d]
    print "EXEC: %s" % cmd
    handle = subprocess.Popen(cmd, stdout=subprocess.PIPE)
    stdout, stderr = handle.communicate()
    
    #Stdout Example:
    #Job <6265891> is submitted to queue <bhour>.
    
    bjob_id_pattern = re.compile("Job <(\\d+)> is submitted to queue.*")    
    m = bjob_id_pattern.match(stdout)
    if m == None:
      raise Exception("Could not parse output from bsub: %s"%stdout)
      
    lsf_job_id = m.group(1)
    with open("%s/lsf_job_id.txt" % d, "w") as fd:
      fd.write(lsf_job_id)
    
  def kill(self, task):
    raise Exception("bkill %s" % task.external_id)

job_queue = LsfQueue()
flock_home = os.path.dirname(os.path.realpath(__file__))
modified_env=dict(os.environ)
modified_env['FLOCK_HOME'] = flock_home

def run(run_id, script, args):
  if(os.path.exists(run_id)):
    print "\"%s\" already exists. Aborting." % run_id
    sys.exit(1)
  
  os.makedirs("%s/temp" % run_id)
  temp_run_script = "%s/temp/run_script.R"%run_id
  with open(temp_run_script, "w") as fd:
    fd.write("source('%s/flock_support.R');\n" % flock_home)
    fd.write("source('%s');\n" % script)
  system("R --vanilla --args %s < %s" % (" ".join(args), temp_run_script))
  poll(run_id)

def poll(run_id):
  while True:
    submitted_count = submit_created(run_id)
    if submitted_count == 0:
      break

def submit_created(run_id):
  submitted_count = 0
  tasks = job_queue.find_tasks(run_id)
  print "tasks: %s" % tasks
  for task in tasks:
    if task.status == CREATED:
      job_queue.submit(task)
      submitted_count += 1
  return submitted_count

def kill(run_id):
  tasks = job_queue.find_tasks(run_id)
  for task in tasks:
    if task.status == SUBMITTED:
      job_queue.kill(task)

#def check(run_id):
#  tasks = find_tasks(run_id)
#  for task in tasks:
    #if task.status == CREATED:
    #  submit(task)

if __name__ == "__main__":
  parser = argparse.ArgumentParser()
  parser.add_argument('--local', help='foo help', action='store_true')
  parser.add_argument('command', help='bar help')
  parser.add_argument('run_id', help='bar help')
  parser.add_argument('arg', nargs='*', help='bar help')
  
  args = parser.parse_args()
  
  #print args
  #print "flock_home=%s" % flock_home
  command = args.command
  modified_env['FLOCK_RUN_DIR'] = os.path.abspath(args.run_id)
  if command == "run":
    run(args.run_id, args.arg[0], args.arg[1:])
  elif command == "kill":
    kill(args.run_id)
  elif command == "poll":
    poll(args.run_id)
  else:
    raise Exception("Unknown command: %s" % command)

#command = sys.argv[1]
#if command == "run":